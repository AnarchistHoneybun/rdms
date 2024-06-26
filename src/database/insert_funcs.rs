use crate::column::{ColumnDataType, Value};
use crate::database::Database;
use crate::database::db_errors::Error;
use crate::table::table_errors;

impl Database {
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
}