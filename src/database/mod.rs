mod errors;

use std::collections::HashMap;
use crate::database::errors::Error;
use crate::table::Table;

pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
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
}