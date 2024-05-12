use crate::column::{ColumnDataType, Value};
use crate::table::helpers::evaluate_nested_conditions;
use crate::table::{Error, NestedCondition, Table};

impl Table {
    /// Updates the values of a specified column with a new value.
    ///
    /// # Arguments
    ///
    /// * `column_name` - A string slice representing the name of the column to update.
    /// * `new_value` - A string slice representing the new value to be set for the column.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the update operation is successful.
    /// * `Err(Error)` if an error occurs during the update operation.
    ///
    /// # Errors
    ///
    /// This function can return the following error:
    ///
    /// * `Error::NonExistingColumn` - If the specified column does not exist in the table.
    /// * `Error::ParseError` - If the new value cannot be parsed into the corresponding column's data type.
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
    /// // Update the "age" column with the value 30
    /// table.update_column("age", "30").unwrap();
    /// ```
    pub fn update_column(&mut self, column_name: &str, new_value: &str) -> Result<(), Error> {
        // Check if the requested column is the primary key column
        if let Some(primary_key_column) = &self.primary_key_column {
            if primary_key_column.name == column_name {
                return Err(Error::CannotBatchUpdatePrimaryKey);
            }
        }

        let update_column = self
            .columns
            .iter_mut()
            .find(|c| c.name == column_name)
            .ok_or(Error::NonExistingColumn(column_name.to_string()))?;

        let new_value = match update_column.data_type {
            ColumnDataType::Integer => new_value
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| Error::ParseError(0, new_value.to_string()))?,
            ColumnDataType::Float => new_value
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| Error::ParseError(0, new_value.to_string()))?,
            ColumnDataType::Text => Value::Text(new_value.to_string()),
        };

        update_column.data = vec![new_value.clone(); update_column.data.len()];

        Ok(())
    }

    /// Updates a column with a new value based on a nested condition structure.
    ///
    /// # Arguments
    ///
    /// * `update_input` - A tuple containing the column name to update and the new value.
    /// * `nested_condition` - A `NestedCondition` enum representing the nested condition structure.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the update operation is successful.
    /// * `Err(Error)` if an error occurs during the update operation.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `Error::NonExistingColumn` - If the column to be updated does not exist in the table.
    /// * `Error::ParseError` - If the new value cannot be parsed into the data type of the requested column.
    /// * `Error::NonExistingColumn` - If a column in the condition does not exist in the table.
    /// * `Error::InvalidOperator` - If an invalid operator is used in the condition.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::{NestedCondition, Table};
    ///
    /// let mut table = Table::new(
    ///     "users",
    ///     vec![
    ///         Column::new("user_id", ColumnDataType::Integer, None),
    ///         Column::new("user_name", ColumnDataType::Text, None),
    ///         Column::new("age", ColumnDataType::Integer, None),
    ///     ],
    /// );
    ///
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "27".to_string()]).unwrap();
    /// table.insert(vec!["2".to_string(), "Bob".to_string(), "35".to_string()]).unwrap();
    /// table.insert(vec!["3".to_string(), "Charlie".to_string(), "19".to_string()]).unwrap();
    ///
    /// // Update the "user_name" column with "Sam" for records where "age" is 30
    /// let nested_condition = NestedCondition::Condition(
    ///     "age".to_string(),
    ///     "=".to_string(),
    ///     "30".to_string(),
    /// );
    /// table.update_with_nested_conditions(
    ///     ("user_name".to_string(), "Sam".to_string()),
    ///     nested_condition,
    /// ).unwrap();
    ///
    /// // Update the "user_name" column with "Sam" for records where "age" is 30 AND "user_id" is 2 OR 3
    /// let nested_condition = NestedCondition::And(
    ///     Box::new(NestedCondition::Condition(
    ///         "age".to_string(),
    ///         "=".to_string(),
    ///         "30".to_string(),
    ///     )),
    ///     Box::new(NestedCondition::Or(
    ///         Box::new(NestedCondition::Condition(
    ///             "user_id".to_string(),
    ///             "=".to_string(),
    ///             "2".to_string(),
    ///         )),
    ///         Box::new(NestedCondition::Condition(
    ///             "user_id".to_string(),
    ///             "=".to_string(),
    ///             "3".to_string(),
    ///         )),
    ///     )),
    /// );
    /// table.update_with_nested_conditions(
    ///     ("user_name".to_string(), "Sam".to_string()),
    ///     nested_condition,
    /// ).unwrap();
    /// ```
    pub fn update_with_nested_conditions(
        &mut self,
        update_input: (String, String),
        nested_condition: NestedCondition,
    ) -> Result<(), Error> {
        // Make a copy of the table so we can restore if needed
        let table_copy = self.copy();

        // Validate column name in update_input
        let update_column = self
            .columns
            .iter()
            .find(|c| c.name == update_input.0)
            .ok_or(Error::NonExistingColumn(update_input.0.clone()))?;

        // Parse new_value according to the column's data type
        let new_value = match update_column.data_type {
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
            ColumnDataType::Text => Value::Text(update_input.1),
        };

        let update_column_name = self
            .columns
            .iter()
            .find(|c| c.name == update_input.0)
            .ok_or(Error::NonExistingColumn(update_input.0.clone()))?
            .name
            .clone();

        let columns_clone = self.columns.clone();

        for record in &mut self.columns {
            if record.name == update_column_name {
                record.data = record.data.iter().enumerate().try_fold(
                    Vec::new(),
                    |mut acc, (i, value)| {
                        let update_record =
                            evaluate_nested_conditions(&nested_condition, &columns_clone, i)?;

                        if update_record {
                            acc.push(new_value.clone());
                        } else {
                            acc.push(value.clone());
                        }

                        Ok(acc)
                    },
                )?;

                // If the updated column is the primary key column, check for duplicates
                if record.is_primary_key {
                    let mut duplicate_found = false;
                    for v in &record.data {
                        if record.data.iter().filter(|&x| *x == *v).count() > 1 {
                            duplicate_found = true;
                            break;
                        }
                    }

                    if duplicate_found {
                        self.columns = table_copy.columns;
                        return Err(Error::DuplicatePrimaryKey);
                    }
                }
            }
        }

        Ok(())
    }
}
