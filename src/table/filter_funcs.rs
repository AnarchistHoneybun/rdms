use crate::table::helpers::evaluate_nested_conditions;
use crate::table::{Error, NestedCondition, Table};
use std::collections::HashSet;

impl Table {
    /// Filters the table rows based on the provided nested condition structure and projects the filtered rows with the specified columns.
    ///
    /// # Arguments
    ///
    /// * `column_names` - A vector of strings representing the names of the columns to project.
    /// * `nested_condition` - A `NestedCondition` enum representing the nested condition structure.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the filtering and projection operation is successful.
    /// * `Err(Error)` if an error occurs during the operation.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `Error::NonExistingColumns` - If one or more of the provided column names do not exist in the table.
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
    ///         Column::new("score", ColumnDataType::Float, None),
    ///     ],
    /// );
    ///
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string(), "85.5".to_string()]).unwrap();
    /// table.insert(vec!["2".to_string(), "Bob".to_string(), "30".to_string(), "92.0".to_string()]).unwrap();
    /// table.insert(vec!["3".to_string(), "Charlie".to_string(), "35".to_string(), "75.0".to_string()]).unwrap();
    ///
    /// // Filter rows where age is greater than 25 and project only id and name columns
    /// let nested_condition = NestedCondition::Condition(
    ///     "age".to_string(),
    ///     ">".to_string(),
    ///     "25".to_string(),
    /// );
    /// table.filter_and_project(
    ///     vec!["user_id".to_string(), "user_name".to_string()],
    ///     nested_condition,
    /// ).unwrap();
    /// ```
    pub fn filter_and_project(
        &self,
        column_names: Vec<String>,
        nested_condition: NestedCondition,
    ) -> Result<(), Error> {
        // Create a new table with the same columns and data types
        let mut filtered_table = self.copy();

        // Filter the rows based on the nested condition
        let columns_clone = self.columns.clone();
        let mut row_indices_to_remove = Vec::new();

        for (row_idx, _) in self.columns[0].data.iter().enumerate() {
            let satisfies_condition =
                evaluate_nested_conditions(&nested_condition, &columns_clone, row_idx)?;

            if !satisfies_condition {
                row_indices_to_remove.push(row_idx);
            }
        }

        for column in &mut filtered_table.columns {
            column.data = column
                .data
                .iter()
                .enumerate()
                .filter_map(|(idx, value)| {
                    if !row_indices_to_remove.contains(&idx) {
                        Some(value.clone())
                    } else {
                        None
                    }
                })
                .collect();
        }

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

        // Project the filtered table with the provided column names
        filtered_table.project(column_names)?;

        Ok(())
    }

    /// Filters the table rows based on the provided nested condition structure and prints the filtered rows to the console.
    ///
    /// # Arguments
    ///
    /// * `nested_condition` - A `NestedCondition` enum representing the nested condition structure.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the filtering operation is successful.
    /// * `Err(Error)` if an error occurs during the filtering operation.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
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
    ///         Column::new("score", ColumnDataType::Float, None),
    ///     ],
    /// );
    ///
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string(), "85.5".to_string()]).unwrap();
    /// table.insert(vec!["2".to_string(), "Bob".to_string(), "30".to_string(), "92.0".to_string()]).unwrap();
    /// table.insert(vec!["3".to_string(), "Charlie".to_string(), "35".to_string(), "75.0".to_string()]).unwrap();
    ///
    /// // Filter rows where age is greater than 25 and score is greater than or equal to 85.0
    /// let nested_condition = NestedCondition::And(
    ///     Box::new(NestedCondition::Condition(
    ///         "age".to_string(),
    ///         ">".to_string(),
    ///         "25".to_string(),
    ///     )),
    ///     Box::new(NestedCondition::Condition(
    ///         "score".to_string(),
    ///         ">=".to_string(),
    ///         "85.0".to_string(),
    ///     )),
    /// );
    /// table.filter_with_nested_conditions(nested_condition).unwrap();
    /// ```
    pub fn filter_with_nested_conditions(
        &self,
        nested_condition: NestedCondition,
    ) -> Result<(), Error> {
        let columns_clone = self.columns.clone();

        // Find the maximum length of column names
        let max_column_name_len = self
            .columns
            .iter()
            .map(|column| column.name.len())
            .max()
            .unwrap_or(0);

        // Print the column names
        for column in &self.columns {
            let padded_name = format!("{:>width$}", column.name, width = max_column_name_len);
            print!("{} ", padded_name);
        }
        println!();

        // Print a separator line
        let separator_line: String = std::iter::repeat("-")
            .take(max_column_name_len * self.columns.len() + self.columns.len() - 1)
            .collect();
        println!("{}", separator_line);

        // Iterate over each row and print rows that satisfy the conditions
        for (row_idx, _) in self.columns[0].data.iter().enumerate() {
            let satisfies_condition =
                evaluate_nested_conditions(&nested_condition, &columns_clone, row_idx)?;

            if satisfies_condition {
                for (_col_idx, column) in self.columns.iter().enumerate() {
                    if row_idx < column.data.len() {
                        let value = &column.data[row_idx];
                        let padded_value =
                            format!("{:>width$}", value, width = max_column_name_len);
                        print!("{} ", padded_value);
                    } else {
                        let padding = " ".repeat(max_column_name_len);
                        print!("{} ", padding);
                    }
                }
                println!();
            }
        }

        Ok(())
    }
}
