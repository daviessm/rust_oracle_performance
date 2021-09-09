use crate::tables::OracleColumn;
use args::Args;
use futures::executor::block_on;
use oracle::{
    sql_type::{OracleType, Timestamp},
    Connector, Privilege, StmtParam,
};
use r2d2_oracle::OracleConnectionManager;
use std::sync::Arc;
use tokio::runtime::Builder;

mod args;
pub mod tables;

fn main() -> Result<(), oracle::Error> {
    let args = Args::parse().unwrap();

    let mut connector = Connector::new(args.username(), args.password(), args.dbname());
    if args.username().to_lowercase().contains("sysdba") {
        connector.privilege(Privilege::Sysdba);
    }

    //Set up the database for testing
    {
        println!("Connecting to Oracle");
        let con = connector.connect().unwrap();
        println!("Connection successful");

        //Build SQL to create a test table
        let mut table_ddl = "create table test1 (id number(18) not null,".to_owned();
        for i in 1..51 {
            table_ddl = format!("{} col{} varchar2(4000),", table_ddl, i);
        }
        table_ddl.push_str(" constraint pk_test1 primary key (id))");
        println!("DDL SQL: {}", &table_ddl);
        match con.execute(&table_ddl, &[]) {
            Err(_) => println!("Unable to create table test1; already exists?"),
            Ok(_) => {
                println!("Created table test1");
                //Insert sample rows
                con.execute("truncate table test1", &[])?;
                let mut insert_sql = "insert into test1 (id,".to_owned();
                for i in 1..51 {
                    insert_sql = format!("{} col{},", insert_sql, i);
                }
                insert_sql = insert_sql.to_string()[..insert_sql.len() - 1].to_owned();
                insert_sql.push_str(") values (0,");
                for i in 1..51 {
                    insert_sql = format!("{} {},", insert_sql, i);
                }
                insert_sql = insert_sql.to_string()[..insert_sql.len() - 1].to_owned();
                insert_sql.push(')');
                println!("Initial insert SQL: {}", &insert_sql);
                con.execute(&insert_sql, &[])?;
                con.commit()?;

                //Duplicate up the rows
                println!("Duplicating up to {} rows", args.rows());
                let mut j = 0;
                while j < args.rows() {
                    let mut duplicate_rows_sql =
                        "insert into test1 select * from (select id + level + ".to_owned();
                    duplicate_rows_sql = format!("{}{},", duplicate_rows_sql, j);
                    for i in 1..51 {
                        duplicate_rows_sql = format!(
                            "{} 'row'||(level+{})||'col'||col{},",
                            duplicate_rows_sql, j, i
                        );
                    }
                    duplicate_rows_sql =
                        duplicate_rows_sql.to_string()[..duplicate_rows_sql.len() - 1].to_owned();
                    duplicate_rows_sql.push_str(
                        " from (select * from test1 where id = 0) connect by level <= 10000)",
                    );
                    //println!("Duplicate rows SQL: {}", &duplicate_rows_sql);
                    con.execute(&duplicate_rows_sql, &[])?;
                    con.commit()?;
                    j = j + 10000;
                }
            }
        }
    }
    println!("Table configured for testing");

    block_on(run_tables(args, connector)).unwrap();
    Ok(())
}

async fn run_tables(args: Args, connector: Connector) -> Result<(), oracle::Error> {
    let connection_pool = r2d2::Pool::builder()
        .max_size(match args.num_threads() {
            Ok(val) => match val {
                Some(val) => val as u32 + 1,
                None => num_cpus::get() as u32 + 1,
            },
            Err(_) => num_cpus::get() as u32 + 1,
        })
        .build(OracleConnectionManager::from_connector(connector.clone()))
        .unwrap();

    let conn = connection_pool.get().unwrap();
    let num_threads = args
        .num_threads()
        .unwrap()
        .or(Some(num_cpus::get()))
        .unwrap();

    let thread_pool = Arc::new(
        Builder::new_multi_thread()
            .thread_name("processor")
            .worker_threads(num_threads)
            .build()
            .unwrap(),
    );

    let all_tab_cols_rows = conn.query("select column_name, data_type from user_tab_columns where table_name = 'TEST1' order by column_id", &[]).unwrap();
    let mut columns = vec![];
    for all_tab_cols_row_result in all_tab_cols_rows {
        let all_tab_cols_row = all_tab_cols_row_result.unwrap();
        let name: String = all_tab_cols_row.get(0).unwrap();
        let data_type = all_tab_cols_row.get(1).unwrap();
        columns.push(OracleColumn::new(name, data_type));
    }

    let mut processor_futures = vec![];

    //Calculate the size of each batch
    let size = args.rows() / num_threads;
    let mut j = 1;
    while j < args.rows() {
        processor_futures.push(thread_pool.spawn(process_table_part(
            connection_pool.clone(),
            j,
            size,
            columns.clone(),
        )));
        j = j + size;
    }

    Ok(for future in processor_futures {
        future.await.unwrap().unwrap();
    })
}

async fn process_table_part(
    connection_pool: r2d2::Pool<OracleConnectionManager>,
    start_id: usize,
    slice_size: usize,
    columns: Vec<OracleColumn>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "In thread processing from id {} to {}",
        start_id,
        start_id + slice_size
    );
    match connection_pool.get() {
        Ok(conn) => {
            let mut this_table_rows_query: String = "select ".to_owned();
            for column in columns {
                this_table_rows_query = format!("{} t.\"{}\"", this_table_rows_query, &column.name);
                if column.data_type == "XMLTYPE" {
                    this_table_rows_query.push_str(".getClobVal()");
                }
                this_table_rows_query =
                    format!("{} as \"{}\", ", this_table_rows_query, &column.name);
            }
            this_table_rows_query =
                this_table_rows_query.to_string()[..this_table_rows_query.len() - 2].to_owned();

            this_table_rows_query.push_str(" from test1 t where id >= :1 and id < :2");
            //println!("{:?}", &this_table_rows_query);

            let mut prepared_statement = conn
                .prepare(&this_table_rows_query, &[StmtParam::FetchArraySize(200)])
                .unwrap();

            let result = prepared_statement.query(&[&start_id, &(start_id + slice_size)])?;

            let mut rowid: String = String::new();
            let mut _i = 1;
            for table_rows_result in result {
                let table_rows_result_row = table_rows_result.unwrap();
                let sql_values = table_rows_result_row.sql_values();
                for column in sql_values {
                    if column.is_null().unwrap() {
                        continue;
                    }
                    match column.oracle_type().unwrap() {
                        OracleType::Rowid => {
                            rowid = column.to_string();
                        }
                        OracleType::Varchar2(_)
                        | OracleType::NVarchar2(_)
                        | OracleType::Char(_)
                        | OracleType::NChar(_)
                        | OracleType::CLOB
                        | OracleType::NCLOB => match column.get::<String>() {
                            Ok(_) => {}
                            Err(e) => println!("Error {} getting string for row {}", e, &rowid),
                        },
                        OracleType::Raw(_) | OracleType::BLOB => match column.get::<Vec<u8>>() {
                            Ok(_) => {}
                            Err(e) => println!("Error {} getting BLOB for row {}", e, &rowid),
                        },
                        OracleType::Number(_, _) | OracleType::Float(_) => {
                            match column.get::<f32>() {
                                Ok(_) => {}
                                Err(e) => println!("Error {} getting float for row {}", e, &rowid),
                            }
                        }
                        OracleType::Int64 => match column.get::<i64>() {
                            Ok(_) => {}
                            Err(e) => println!("Error {} getting int in for row {}", e, &rowid),
                        },
                        OracleType::Date
                        | OracleType::Timestamp(_)
                        | OracleType::TimestampTZ(_)
                        | OracleType::TimestampLTZ(_) => match column.get::<Timestamp>() {
                            Ok(_) => {}
                            Err(e) => println!("Error {} getting timestamp for row {}", e, &rowid),
                        },
                        _ => {
                            println!(
                                "Unhandled Oracle type {}, column {}",
                                column.oracle_type().unwrap(),
                                _i
                            )
                        }
                    }
                }
                //debug!("{} at {}+{}: {}", table.name, start_rowid, _i, rowid);
                _i += 1;
            }
            println!(
                "Finished thread processing from row {} to {}",
                &start_id,
                &(start_id + slice_size)
            );
        }
        Err(e) => println!("Error connecting: {}", e),
    }
    Ok(())
}
