mod export_import;
mod helpers;
mod filter_funcs;
mod insert_funcs;
mod update_funcs;
mod table_utils;

use crate::column::{Column};
use std::{fmt};

#[derive(Debug)]
pub enum NestedCondition {
    Condition(String, String, String),
    And(Box<NestedCondition>, Box<NestedCondition>),
    Or(Box<NestedCondition>, Box<NestedCondition>),
}

/// This Operator enum represents the different comparison operators that can be used in an update
/// or select condition. These are mapped to respective operations on execution.
#[derive(Debug, PartialEq)]
enum Operator {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl Operator {
    /// This function converts a string to an Operator enum. It returns an error if the requested string
    /// is not a supported operator.
    fn from_str(s: &str) -> Result<Operator, String> {
        match s {
            "=" => Ok(Operator::Equal),
            "!=" => Ok(Operator::NotEqual),
            "<" => Ok(Operator::LessThan),
            ">" => Ok(Operator::GreaterThan),
            "<=" => Ok(Operator::LessThanOrEqual),
            ">=" => Ok(Operator::GreaterThanOrEqual),
            _ => Err(format!("Invalid operator: {}", s)),
        }
    }
}

/// Enum for various error types that can occur during table operations.
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
}

/// Implement Display trait for Error enum to allow for custom error messages.
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        }
    }
}

impl std::error::Error for Error {}

/// Struct representing a table with a name and a vector of columns
/// (data is stored inside the column struct).
#[derive(Debug)]
pub struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) primary_key_column: Option<Column>,
}

impl Table {
    /// Creates a new `Table` instance with the provided table name and columns.
    ///
    /// # Arguments
    ///
    /// * `table_name` - A string slice representing the name of the table.
    /// * `columns` - A vector of `Column` instances representing the columns in the table.
    ///
    /// # Returns
    ///
    /// A `Table` instance with the provided name and columns.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let columns = vec![
    ///     Column::new("id", ColumnDataType::Integer, None),
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ];
    ///
    /// let table = Table::new("users", columns);
    /// ```
    pub fn new(table_name: &str, columns: Vec<Column>) -> Result<Table, Error> {
        let mut primary_key_column: Option<Column> = None;

        // Validate that only one column is marked as the primary key
        for column in &columns {
            if column.is_primary_key {
                if primary_key_column.is_some() {
                    return Err(Error::MultiplePrimaryKeys);
                }
                primary_key_column = Some(column.clone());
            }
        }

        Ok(Table {
            name: table_name.to_string(),
            columns,
            primary_key_column,
        })
    }

}
