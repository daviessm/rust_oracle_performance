#[derive(Debug, Eq, Clone)]
pub struct OracleColumn {
    pub name: String,
    pub data_type: String,
}

impl OracleColumn {
    pub fn new(name: String, data_type: String) -> Self {
        Self { name, data_type }
    }
}

impl PartialEq for OracleColumn {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name) && self.data_type.eq(&other.data_type)
    }
}
