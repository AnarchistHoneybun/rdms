pub(crate) mod errors;
mod export_import;
mod filter_funcs;
mod helpers;
mod insert_funcs;
mod operators;
mod table_utils;
mod update_funcs;

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
