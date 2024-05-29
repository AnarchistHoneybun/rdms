mod delete_funcs;
mod export_import;
mod filter_funcs;
mod helpers;
mod insert_funcs;
pub(crate) mod operators;
pub(crate) mod table_errors;
mod table_utils;
mod update_funcs;

use crate::column::Column;
use crate::table::table_errors::Error;

#[derive(Debug)]
pub enum NestedCondition {
    Condition(String, String, String),
    And(Box<NestedCondition>, Box<NestedCondition>),
    Or(Box<NestedCondition>, Box<NestedCondition>),
}

/// Struct representing a table with a name and a vector of columns
/// (data is stored inside the column struct).
#[derive(Debug, Clone)]
pub struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) primary_key_column: Option<Column>,
    pub(crate) referenced_as_foreign_key: Vec<(String, String)>,
}
