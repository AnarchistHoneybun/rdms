mod db_errors;

use crate::column::Column;
use crate::database::db_errors::Error;
use crate::table::Table;
use std::collections::HashMap;

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
            }
        }

        let table = Table {
            name: table_name.to_string(),
            columns,
            primary_key_column,
        };

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
