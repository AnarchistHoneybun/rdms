#[derive(Debug)]

pub enum Error {
    TableAlreadyExists(String),
    MultiplePrimaryKeys,
    ReferencedTableNotFound(String),
    ReferencedColumnNotFound(String, String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::TableAlreadyExists(table_name) => {
                write!(f, "Table '{}' already exists in this database", table_name)
            }
            Error::MultiplePrimaryKeys => {
                write!(f, "Multiple columns marked as primary keys")
            }
            Error::ReferencedTableNotFound(table_name) => {
                write!(
                    f,
                    "Referenced table '{}' not found in the database",
                    table_name
                )
            }
            Error::ReferencedColumnNotFound(table_name, column_name) => {
                write!(
                    f,
                    "Referenced column '{}' not found in table '{}'",
                    column_name, table_name
                )
            }
        }
    }
}

impl std::error::Error for Error {}
