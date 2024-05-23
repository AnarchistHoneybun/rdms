use crate::table::table_errors;

#[derive(Debug)]

pub enum Error {
    TableAlreadyExists(String),
    TableNotFound(String),
    MultiplePrimaryKeys,
    ReferencedTableNotFound(String),
    ReferencedColumnNotFound(String, String),
    NullForeignKey(String),
    ForeignKeyViolation(String, String, String),
    MissingForeignKeyColumns(Vec<String>),
    ParseError(usize, String),
    TableError(table_errors::Error),
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
            Error::TableNotFound(table_name) => {
                write!(f, "Table '{}' not found in this database", table_name)
            }
            Error::NullForeignKey(column_name) => {
                write!(f, "Foreign key column '{}' cannot be null", column_name)
            }
            Error::ForeignKeyViolation(value, column_name, reference_table) => {
                write!(
                    f,
                    "Foreign key violation: value '{}' in column '{}' does not exist in table '{}'",
                    value, column_name, reference_table
                )
            }
            Error::MissingForeignKeyColumns(columns) => {
                write!(
                    f,
                    "The following foreign key columns are missing: {}",
                    columns.join(", ")
                )
            }
            Error::ParseError(index, value) => {
                write!(f, "Failed to parse value '{}' at index {}", value, index)
            }
            Error::TableError(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for Error {}

impl From<table_errors::Error> for Error {
    fn from(err: table_errors::Error) -> Self {
        match err {
            table_errors::Error::MismatchedColumnCount => {
                Error::TableError(table_errors::Error::MismatchedColumnCount)
            }
            table_errors::Error::ParseError(index, value) => {
                Error::TableError(table_errors::Error::ParseError(index, value))
            }
            table_errors::Error::NonExistingColumns(columns) => {
                Error::TableError(table_errors::Error::NonExistingColumns(columns))
            }
            table_errors::Error::NonExistingColumn(column_name) => {
                Error::TableError(table_errors::Error::NonExistingColumn(column_name))
            }
            table_errors::Error::InvalidOperator(operator_str) => {
                Error::TableError(table_errors::Error::InvalidOperator(operator_str))
            }
            table_errors::Error::FileError(msg) => {
                Error::TableError(table_errors::Error::FileError(msg))
            }
            table_errors::Error::InvalidFormat(format) => {
                Error::TableError(table_errors::Error::InvalidFormat(format))
            }
            table_errors::Error::MultiplePrimaryKeys => {
                Error::TableError(table_errors::Error::MultiplePrimaryKeys)
            }
            table_errors::Error::DuplicatePrimaryKey => {
                Error::TableError(table_errors::Error::DuplicatePrimaryKey)
            }
            table_errors::Error::NullPrimaryKey => {
                Error::TableError(table_errors::Error::NullPrimaryKey)
            }
            table_errors::Error::CannotBatchUpdatePrimaryKey => {
                Error::TableError(table_errors::Error::CannotBatchUpdatePrimaryKey)
            }
            table_errors::Error::PrimaryKeyNotProvided(column_name) => {
                Error::TableError(table_errors::Error::PrimaryKeyNotProvided(column_name))
            }
        }
    }
}
