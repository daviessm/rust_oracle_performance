use anyhow::{Context, Result};
use clap::{App, Arg};

pub struct Args {
    inner: clap::ArgMatches<'static>,
}

impl Args {
    pub fn parse() -> Result<Self> {
        let inner = App::new("rust_oracle_performance")
            .version(env!("CARGO_PKG_VERSION"))
            .arg(
                Arg::with_name("username")
                    .long("username")
                    .short("u")
                    .takes_value(true)
                    .value_name("USER")
                    .required(true)
                    .help("The username to connect as"),
            )
            .arg(
                Arg::with_name("password")
                    .long("password")
                    .short("p")
                    .takes_value(true)
                    .value_name("PASS")
                    .required(true)
                    .help("The password"),
            )
            .arg(
                Arg::with_name("dbname")
                    .long("dbname")
                    .short("db")
                    .takes_value(true)
                    .value_name("DB")
                    .required(true)
                    .help("The database service name"),
            )
            .arg(
                Arg::with_name("threads")
                    .long("threads")
                    .short("t")
                    .takes_value(true)
                    .value_name("NTHREADS")
                    .help(
                        "The maximum number of table segments to process\n\
                         at one time (default is number of CPUs - 1)",
                    ),
            )
            .arg(
                Arg::with_name("rows")
                    .long("rows")
                    .short("r")
                    .takes_value(true)
                    .value_name("NROWS")
                    .default_value("2000000")
                    .help("The number of tabl rows to test with (default 2000000)"),
            )
            .get_matches();
        Ok(Self { inner })
    }

    pub fn num_threads(&self) -> Result<Option<usize>> {
        if let Some(num_row_threads_str) = self.inner.value_of("threads") {
            Ok(Some(
                num_row_threads_str
                    .parse()
                    .context("Failed to parse num threads.")?,
            ))
        } else {
            Ok(None)
        }
    }

    pub fn username(&self) -> &str {
        self.inner.value_of("username").unwrap()
    }

    pub fn password(&self) -> &str {
        self.inner.value_of("password").unwrap()
    }

    pub fn dbname(&self) -> &str {
        self.inner.value_of("dbname").unwrap()
    }

    pub fn rows(&self) -> usize {
        self.inner
            .value_of("rows")
            .unwrap()
            .parse()
            .context("Failed to parse num rows")
            .unwrap()
    }
}
