use std::collections::HashSet;
use crate::column::{ColumnDataType, Value};
use crate::table::{Error, Table};

impl Table {
    /// Inserts a new record into the table.
    ///
    /// # Arguments
    ///
    /// * `data` - A vector of `String` values representing the data to be inserted. The number of values must match the number of columns in the table.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the insertion operation is successful.
    /// * `Err(Error)` if an error occurs during the insertion operation.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `Error::MismatchedColumnCount` - If the number of provided data values does not match the number of columns in the table.
    /// * `Error::ParseError` - If a data value cannot be parsed into the corresponding column's data type.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let mut table = Table::new("users", vec![
    ///     Column::new("id", ColumnDataType::Integer, None),
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]);
    ///
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string()]).unwrap();
    /// ```
    pub fn insert(&mut self, data: Vec<String>) -> Result<(), Error> {
        if data.len() != self.columns.len() {
            return Err(Error::MismatchedColumnCount);
        }

        let mut parsed_values: Vec<Value> = Vec::with_capacity(self.columns.len());

        for (column, value_str) in self.columns.iter().zip(data.into_iter()) {
            if value_str.trim().to_lowercase() == "null" {
                parsed_values.push(Value::Null);
            } else {
                match column.data_type {
                    ColumnDataType::Integer => match value_str.parse::<i64>() {
                        Ok(value) => parsed_values.push(Value::Integer(value)),
                        Err(_) => return Err(Error::ParseError(parsed_values.len(), value_str)),
                    },
                    ColumnDataType::Float => match value_str.parse::<f64>() {
                        Ok(value) => parsed_values.push(Value::Float(value)),
                        Err(_) => return Err(Error::ParseError(parsed_values.len(), value_str)),
                    },
                    ColumnDataType::Text => parsed_values.push(Value::Text(value_str)),
                }
            }
        }

        // Check if the primary key column exists and validate the primary key value
        if let Some(primary_key_column) = &self.primary_key_column {
            let primary_key_idx = self
                .columns
                .iter()
                .position(|c| c.name == primary_key_column.name)
                .unwrap();
            let primary_key_value = &parsed_values[primary_key_idx];

            if primary_key_value == &Value::Null {
                return Err(Error::NullPrimaryKey);
            }

            // Check for duplicate primary key values
            for column in &self.columns {
                if column.name == primary_key_column.name {
                    if column.data.contains(primary_key_value) {
                        return Err(Error::DuplicatePrimaryKey);
                    }
                }
            }
        }

        for (column, value) in self.columns.iter_mut().zip(parsed_values.into_iter()) {
            column.data.push(value);
        }

        Ok(())
    }

    /// Function to insert a new record, but can be formatted to only insert data into specific columns.
    /// Will fill the other columns with a null value.
    ///
    /// # Arguments
    ///
    /// * `column_names` - A vector of strings representing the names of the columns to insert data into.
    /// * `data` - A vector of strings representing the data to be inserted.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the insert operation is successful.
    /// * `Err(Error)` if an error occurs during the insert operation.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `Error::NonExistingColumns` - If one or more of the provided column names do not exist in the table.
    /// * `Error::MismatchedColumnCount` - If the number of provided data items does not match the number of provided column names.
    /// * `Error::ParseError` - If a data item cannot be parsed into the corresponding column's data type.
    /// # Examples
    ///
    /// ```
    /// use crate::table::Table;
    ///
    /// let mut table = Table::new("users", /* ... */);
    /// // Insert into all columns
    /// table.insert_with_columns(
    ///     vec!["user_id".to_string(), "user_name".to_string(), "age".to_string()],
    ///     vec!["4".to_string(), "David".to_string(), "32".to_string()]
    /// ).unwrap();
    /// // Insert into specific columns
    /// table.insert_with_columns(
    ///     vec!["user_name".to_string(), "age".to_string()],
    ///     vec!["Emily".to_string(), "28".to_string()]
    /// ).unwrap();
    /// ```
    pub fn insert_with_columns(
        &mut self,
        column_names: Vec<String>,
        data: Vec<String>,
    ) -> Result<(), Error> {
        // Check if all provided column names exist in the table
        let column_names_set: HashSet<String> = column_names.iter().cloned().collect();
        let existing_columns: HashSet<String> =
            self.columns.iter().map(|c| c.name.clone()).collect();
        let non_existing_columns: Vec<String> = column_names_set
            .difference(&existing_columns)
            .cloned()
            .collect();

        if !non_existing_columns.is_empty() {
            return Err(Error::NonExistingColumns(non_existing_columns));
        }

        // Check if the number of data items matches the number of provided column names
        if data.len() != column_names.len() {
            return Err(Error::MismatchedColumnCount);
        }

        // Check if the provided columns contain the primary key column
        if let Some(primary_key_column) = &self.primary_key_column {
            if !column_names.contains(&primary_key_column.name) {
                return Err(Error::PrimaryKeyNotProvided(primary_key_column.name.clone()));
            }
        }

        let mut parsed_values: Vec<Value> = vec![Value::Null; self.columns.len()];

        for (column_name, value_str) in column_names.iter().zip(data.into_iter()) {
            if let Some(column_idx) = self.columns.iter().position(|c| c.name == *column_name) {
                let column = &self.columns[column_idx];
                if value_str.trim().to_lowercase() == "null" {
                    parsed_values[column_idx] = Value::Null;
                } else {
                    match column.data_type {
                        ColumnDataType::Integer => match value_str.parse::<i64>() {
                            Ok(value) => parsed_values[column_idx] = Value::Integer(value),
                            Err(_) => return Err(Error::ParseError(column_idx, value_str)),
                        },
                        ColumnDataType::Float => match value_str.parse::<f64>() {
                            Ok(value) => parsed_values[column_idx] = Value::Float(value),
                            Err(_) => return Err(Error::ParseError(column_idx, value_str)),
                        },
                        ColumnDataType::Text => parsed_values[column_idx] = Value::Text(value_str),
                    }
                }
            }
        }

        // Check if the primary key value for the new record already exists
        if let Some(primary_key_column) = &self.primary_key_column {
            let primary_key_idx = self
                .columns
                .iter()
                .position(|c| c.name == primary_key_column.name)
                .unwrap();
            let primary_key_value = &parsed_values[primary_key_idx];

            if primary_key_value != &Value::Null {
                for column in &self.columns {
                    if column.name == primary_key_column.name {
                        if column.data.contains(primary_key_value) {
                            return Err(Error::DuplicatePrimaryKey);
                        }
                    }
                }
            } else {
                return Err(Error::NullPrimaryKey);
            }
        }

        for (column, value) in self.columns.iter_mut().zip(parsed_values.into_iter()) {
            column.data.push(value);
        }

        Ok(())
    }
}