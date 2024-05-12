mod export_import;
mod filter_funcs;
mod helpers;
mod insert_funcs;
mod table_utils;
mod update_funcs;
mod errors;
mod operators;

use crate::column::Column;
use crate::table::errors::Error;

#[derive(Debug)]
pub enum NestedCondition {
    Condition(String, String, String),
    And(Box<NestedCondition>, Box<NestedCondition>),
    Or(Box<NestedCondition>, Box<NestedCondition>),
}

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
