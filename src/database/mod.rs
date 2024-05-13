mod errors;

use crate::database::errors::Error;
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

    pub fn create_table(&mut self, table: Table) -> Result<(), Error> {
        // Check if a table with the same name already exists
        if self.tables.contains_key(&table.name) {
            return Err(Error::TableAlreadyExists(table.name.clone()));
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
