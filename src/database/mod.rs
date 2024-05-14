mod db_errors;

use crate::column::{Column, ColumnDataType, Value};
use crate::database::db_errors::Error;
use crate::table::{Table, table_errors::Error as TableError};
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

        let table= Table::new(table_name, columns).unwrap();

        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    pub fn insert_into_table(
        &mut self,
        table_name: &str,
        data: Vec<String>,
    ) -> Result<(), Error> {

        let copied_tables = self.tables.clone();

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(Error::TableNotFound(table_name.to_owned()))?;

        // Check the foreign key constraints first
        for (column_idx, value_str) in data.iter().enumerate() {
            let column = &table.columns[column_idx];
            if let Some(fk_info) = &column.foreign_key {
                let referenced_table = copied_tables
                    .get(&fk_info.reference_table)
                    .cloned()
                    .ok_or(Error::ReferencedTableNotFound(fk_info.reference_table.clone()))?;

                let referenced_column = referenced_table
                    .columns
                    .iter()
                    .find(|c| c.name == fk_info.reference_column)
                    .ok_or(Error::ReferencedColumnNotFound(
                        fk_info.reference_table.clone(),
                        fk_info.reference_column.clone(),
                    ))?;

                let value = if value_str.trim().to_lowercase() == "null" {
                    Value::Null
                } else {
                    match column.data_type {
                        ColumnDataType::Integer => Value::Integer(value_str.parse().map_err(|_| {
                            Error::ParseError(column_idx, value_str.to_owned())
                        })?),
                        ColumnDataType::Float => Value::Float(value_str.parse().map_err(|_| {
                            Error::ParseError(column_idx, value_str.to_owned())
                        })?),
                        ColumnDataType::Text => Value::Text(value_str.to_owned()),
                    }
                };

                if value == Value::Null {
                    return Err(Error::NullForeignKey(column.name.clone()));
                }

                if !referenced_column.data.contains(&value) {
                    return Err(Error::ForeignKeyViolation(
                        value.to_string(),
                        column.name.clone(),
                        fk_info.reference_table.clone(),
                    ));
                }
            }
        }

        // If all foreign key constraints are satisfied, insert the data into the table
        table.insert(data)?;

        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }

    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_name)
    }
}
