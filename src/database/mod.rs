use std::collections::HashMap;

use crate::column::{Column, ColumnDataType, Value};
use crate::database::db_errors::Error;
use crate::table::{helpers::evaluate_nested_conditions, table_errors, NestedCondition, Table};

mod db_errors;
mod insert_funcs;
mod update_funcs;
mod delete_funcs;

pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, table_name: &str, columns: Vec<Column>) -> Result<(), Error> {
        // Check if a table with the same name already exists
        if self.tables.contains_key(table_name) {
            return Err(Error::TableAlreadyExists(table_name.to_string()));
        }

        let mut primary_key_column: Option<Column> = None;

        // Validate that only one column is marked as the primary key
        for column in &columns {
            if column.is_primary_key {
                if primary_key_column.is_some() {
                    return Err(Error::MultiplePrimaryKeys);
                }
                primary_key_column = Some(column.clone());
            }

            // Validate foreign key references
            if let Some(fk_info) = &column.foreign_key {
                // Check if the referenced table exists in the database
                if !self.tables.contains_key(&fk_info.reference_table) {
                    return Err(Error::ReferencedTableNotFound(
                        fk_info.reference_table.clone(),
                    ));
                }

                // Check if the referenced column exists in the referenced table
                let referenced_table = self.tables.get(&fk_info.reference_table).unwrap(); // Safe to unwrap since we checked for the table's existence
                if !referenced_table
                    .columns
                    .iter()
                    .any(|col| col.name == fk_info.reference_column)
                {
                    return Err(Error::ReferencedColumnNotFound(
                        fk_info.reference_table.clone(),
                        fk_info.reference_column.clone(),
                    ));
                }

                // Check if the referenced column is the primary key column in the referenced table
                if !referenced_table
                    .columns
                    .iter()
                    .any(|col| col.is_primary_key && col.name == fk_info.reference_column)
                {
                    return Err(Error::ReferencedColumnNotPrimaryKey(
                        fk_info.reference_table.clone(),
                        fk_info.reference_column.clone(),
                    ));
                }
            }
        }

        let table = Table::new(table_name, columns).unwrap();

        for (column, fk_info) in table
            .columns
            .iter()
            .filter_map(|col| col.foreign_key.as_ref().map(|fk| (col, fk)))
        {
            if let Some(referenced_table) = self.tables.get_mut(&fk_info.reference_table) {
                referenced_table
                    .referenced_as_foreign_key
                    .push((table.name.clone(), column.name.clone()));
            }
        }

        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }

    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_name)
    }
}
