use crate::column::{ColumnDataType, Value};
use crate::database::Database;
use crate::database::db_errors::Error;
use crate::table::{NestedCondition, table_errors};

impl Database {
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
            .ok_or(Error::TableError(table_errors::Error::NonExistingColumn(
                update_input.0.clone(),
            )))?;

        let is_primary_key_column = update_column.is_primary_key;

        let mut old_primary_key_values: Vec<Value> = Vec::new();

        if is_primary_key_column {
            for pk_value in &update_column.data {
                old_primary_key_values.push(pk_value.clone());
            }
        }

        //dbg!(&old_primary_key_values);

        if let Some(fk_info) = &update_column.foreign_key {
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

        let table_foreign_key_data = table.referenced_as_foreign_key.clone();

        table.update_with_nested_conditions(update_input.clone(), nested_condition)?;

        let mut new_primary_key_values: Vec<Value> = Vec::new();

        if is_primary_key_column {
            for pk_value in &table
                .columns
                .iter()
                .find(|c| c.name == update_input.0)
                .unwrap()
                .data
            {
                new_primary_key_values.push(pk_value.clone());
            }
        }
        //dbg!(&new_primary_key_values);

        let old_pk_value = old_primary_key_values
            .iter()
            .find(|value| !new_primary_key_values.contains(value))
            .cloned();

        let new_pk_value = new_primary_key_values
            .iter()
            .find(|value| !old_primary_key_values.contains(value))
            .cloned();

        //dbg!(&old_pk_value);
        //dbg!(&new_pk_value);

        if is_primary_key_column {
            for (ref_table_name, ref_column_name) in table_foreign_key_data {
                let condition = NestedCondition::Condition(
                    ref_column_name.clone(),
                    "=".to_string(),
                    old_pk_value.clone().unwrap().to_string(),
                );
                self.update_with_nested_conditions_in_table(
                    &ref_table_name,
                    (
                        ref_column_name.clone(),
                        new_pk_value.clone().unwrap().to_string(),
                    ),
                    condition,
                )?;
            }
        }

        Ok(())
    }
}