#[derive(Debug)]

pub enum Error {
    TableAlreadyExists(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::TableAlreadyExists(table_name) => {
                write!(f, "Table '{}' already exists in this database", table_name)
            }
        }
    }
}

impl std::error::Error for Error {}
