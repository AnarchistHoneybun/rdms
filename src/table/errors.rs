#[derive(Debug)]
pub enum Error {
    MismatchedColumnCount,
    ParseError(usize, String),
    NonExistingColumns(Vec<String>),
    NonExistingColumn(String), // column_name
    InvalidOperator(String),   // operator_str
    FileError(String),
    InvalidFormat(String),
    MultiplePrimaryKeys,
    DuplicatePrimaryKey,
    NullPrimaryKey,
    CannotBatchUpdatePrimaryKey,
    PrimaryKeyNotProvided(String), // column_name
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MismatchedColumnCount => {
                write!(f, "Number of values doesn't match the number of columns")
            }
            Error::ParseError(index, value) => {
                write!(f, "Failed to parse value '{}' at index {}", value, index)
            }
            Error::NonExistingColumns(columns) => write!(
                f,
                "The following columns do not exist: {}",
                columns.join(", ")
            ),
            Error::NonExistingColumn(column_name) => {
                write!(f, "The column '{}' does not exist", column_name)
            }
            Error::InvalidOperator(operator_str) => write!(f, "Invalid operator: {}", operator_str),
            Error::FileError(msg) => write!(f, "File error: {}", msg),
            Error::InvalidFormat(format) => write!(f, "Invalid format: {}", format),
            Error::MultiplePrimaryKeys => write!(f, "Multiple primary keys are not allowed"),
            Error::DuplicatePrimaryKey => write!(f, "Duplicate primary key value"),
            Error::NullPrimaryKey => write!(f, "Primary key value cannot be null"),
            Error::CannotBatchUpdatePrimaryKey => {
                write!(f, "Primary key column disallows batch updates")
            }
            Error::PrimaryKeyNotProvided(column_name) => {
                write!(f, "Primary key column '{}' not provided", column_name)
            }
        }
    }
}

impl std::error::Error for Error {}
