use std::collections::HashMap;

use crate::column::{Column, ColumnDataType, Value};
use crate::database::db_errors::Error;
use crate::table::{table_errors, NestedCondition, Table};

mod db_errors;

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

    pub fn insert_into_table(&mut self, table_name: &str, data: Vec<String>) -> Result<(), Error> {
        let copied_tables = self.tables.clone();

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(Error::TableNotFound(table_name.to_owned()))?;

        // Check the foreign key constraints first
        for (column_idx, value_str) in data.iter().enumerate() {
            let column = &table.columns[column_idx];
            if let Some(fk_info) = &column.foreign_key {
                let referenced_table = copied_tables.get(&fk_info.reference_table).cloned().ok_or(
                    Error::ReferencedTableNotFound(fk_info.reference_table.clone()),
                )?;

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
                        ColumnDataType::Integer => Value::Integer(
                            value_str
                                .parse()
                                .map_err(|_| Error::ParseError(column_idx, value_str.to_owned()))?,
                        ),
                        ColumnDataType::Float => Value::Float(
                            value_str
                                .parse()
                                .map_err(|_| Error::ParseError(column_idx, value_str.to_owned()))?,
                        ),
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

    pub fn insert_with_columns_into_table(
        &mut self,
        table_name: &str,
        column_names: Vec<String>,
        data: Vec<String>,
    ) -> Result<(), Error> {
        let copied_tables = self.tables.clone();

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(Error::TableNotFound(table_name.to_owned()))?;

        // Check if all foreign key columns are included in the provided column_names
        let missing_foreign_key_columns: Vec<_> = table
            .columns
            .iter()
            .filter(|col| col.foreign_key.is_some())
            .filter(|col| !column_names.contains(&col.name))
            .map(|col| col.name.clone())
            .collect();

        if !missing_foreign_key_columns.is_empty() {
            return Err(Error::MissingForeignKeyColumns(missing_foreign_key_columns));
        }

        // Check the foreign key constraints for the provided columns
        for (column_idx, value_str) in data.iter().enumerate() {
            let column_name = &column_names[column_idx];
            let column = table
                .columns
                .iter()
                .find(|col| col.name == *column_name)
                .ok_or_else(|| {
                    Error::TableError(table_errors::Error::NonExistingColumn(column_name.clone()))
                })?;

            if let Some(fk_info) = &column.foreign_key {
                let referenced_table = copied_tables.get(&fk_info.reference_table).cloned().ok_or(
                    Error::ReferencedTableNotFound(fk_info.reference_table.clone()),
                )?;

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
                        ColumnDataType::Integer => {
                            Value::Integer(value_str.parse().map_err(|_| {
                                Error::TableError(table_errors::Error::ParseError(
                                    column_idx,
                                    value_str.to_owned(),
                                ))
                            })?)
                        }
                        ColumnDataType::Float => Value::Float(value_str.parse().map_err(|_| {
                            Error::TableError(table_errors::Error::ParseError(
                                column_idx,
                                value_str.to_owned(),
                            ))
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
        table.insert_with_columns(column_names, data)?;

        Ok(())
    }

    pub fn update_column_in_table(
        &mut self,
        table_name: &str,
        column_name: &str,
        new_value: &str,
    ) -> Result<(), Error> {
        let copied_tables = self.tables.clone();

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(Error::TableNotFound(table_name.to_owned()))?;

        // Check if the column is a foreign key column
        let column =
            table
                .columns
                .iter()
                .find(|c| c.name == column_name)
                .ok_or(Error::TableError(table_errors::Error::NonExistingColumn(
                    column_name.to_string(),
                )))?;

        if let Some(fk_info) = &column.foreign_key {
            let referenced_table = copied_tables.get(&fk_info.reference_table).cloned().ok_or(
                Error::ReferencedTableNotFound(fk_info.reference_table.clone()),
            )?;

            let referenced_column = referenced_table
                .columns
                .iter()
                .find(|c| c.name == fk_info.reference_column)
                .ok_or(Error::ReferencedColumnNotFound(
                    fk_info.reference_table.clone(),
                    fk_info.reference_column.clone(),
                ))?;

            let value = if new_value.trim().to_lowercase() == "null" {
                Value::Null
            } else {
                match column.data_type {
                    ColumnDataType::Integer => new_value
                        .parse::<i64>()
                        .map(Value::Integer)
                        .map_err(|_| Error::ParseError(0, new_value.to_string()))?,
                    ColumnDataType::Float => new_value
                        .parse::<f64>()
                        .map(Value::Float)
                        .map_err(|_| Error::ParseError(0, new_value.to_string()))?,
                    ColumnDataType::Text => Value::Text(new_value.to_string()),
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

        table.update_column(column_name, new_value)?;

        Ok(())
    }

    pub fn update_with_nested_conditions_in_table(
        &mut self,
        table_name: &str,
        update_input: (String, String),
        nested_condition: NestedCondition,
    ) -> Result<(), Error> {
        let copied_tables = self.tables.clone();

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(Error::TableNotFound(table_name.to_owned()))?;

        let update_column = table
            .columns
            .iter()
            .find(|c| c.name == update_input.0)
            .ok_or(Error::TableError(table_errors::Error::NonExistingColumn(update_input.0.clone())))?;

        if let Some(fk_info) = &update_column.foreign_key {
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

            let new_value = if update_input.1.trim().to_lowercase() == "null" {
                Value::Null
            } else {
                match update_column.data_type {
                    ColumnDataType::Integer => update_input
                        .1
                        .parse::<i64>()
                        .map(Value::Integer)
                        .map_err(|_| Error::ParseError(1, update_input.1.clone()))?,
                    ColumnDataType::Float => update_input
                        .1
                        .parse::<f64>()
                        .map(Value::Float)
                        .map_err(|_| Error::ParseError(1, update_input.1.clone()))?,
                    ColumnDataType::Text => Value::Text(update_input.1.clone()),
                }
            };

            if new_value == Value::Null {
                return Err(Error::NullForeignKey(update_column.name.clone()));
            }

            if !referenced_column.data.contains(&new_value) {
                return Err(Error::ForeignKeyViolation(
                    new_value.to_string(),
                    update_column.name.clone(),
                    fk_info.reference_table.clone(),
                ));
            }
        }

        table.update_with_nested_conditions(update_input, nested_condition)?;

        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }

    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_name)
    }
}
