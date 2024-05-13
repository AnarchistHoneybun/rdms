use crate::column::{Column, Value};
use crate::table::{Error, Table};
use std::collections::HashSet;

impl Table {
    /// Creates a new `Table` instance with the provided table name and columns.
    ///
    /// # Arguments
    ///
    /// * `table_name` - A string slice representing the name of the table.
    /// * `columns` - A vector of `Column` instances representing the columns in the table.
    ///
    /// # Returns
    ///
    /// * `Ok(Table)` - A `Table` instance with the provided name and columns.
    /// * `Err(Error)` - An error if multiple columns are marked as the primary key.
    ///
    /// # Errors
    ///
    /// This function can return the following error:
    ///
    /// * `Error::MultiplePrimaryKeys` - If more than one column is marked as the primary key.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let columns = vec![
    ///     Column::new("id", ColumnDataType::Integer, Some(true)), // Primary key column
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ];
    ///
    /// let table = Table::new("users", columns).unwrap();
    /// ```
    pub fn new(table_name: &str, columns: Vec<Column>) -> Result<Table, Error> {
        let mut primary_key_column: Option<Column> = None;

        // Validate that only one column is marked as the primary key
        for column in &columns {
            if column.is_primary_key {
                if primary_key_column.is_some() {
                    return Err(Error::MultiplePrimaryKeys);
                }
                primary_key_column = Some(column.clone());
            }
        }

        Ok(Table {
            name: table_name.to_string(),
            columns,
            primary_key_column,
        })
    }

    /// Creates a copy of the current `Table` instance.
    ///
    /// # Returns
    ///
    /// A `Table` instance with the same name, columns, and data as the current instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let mut table = Table::new("users", vec![
    ///     Column::new("id", ColumnDataType::Integer, Some(true)), // Primary key column
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]).unwrap();
    ///
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string()]).unwrap();
    ///
    /// let table_copy = table.copy();
    /// ```
    pub fn copy(&self) -> Table {
        let mut new_columns = Vec::with_capacity(self.columns.len());
        let mut new_primary_key_column: Option<Column> = None;

        for column in &self.columns {
            let mut new_column = Column::new(&*column.name.clone(), column.data_type.clone(), None, column.is_primary_key, None);
            new_column.data = column.data.clone();

            if column.is_primary_key {
                new_primary_key_column = Some(new_column.clone());
            }

            new_columns.push(new_column);
        }

        Table {
            name: self.name.clone(),
            columns: new_columns,
            primary_key_column: new_primary_key_column,
        }
    }

    /// Prints the entire table data to the console.
    ///
    /// The function finds the maximum length of column names to properly align the data, and then prints the column names, a separator line, and the data rows. If the number of rows varies across columns, the function will print blank spaces for missing values.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let mut table = Table::new("users", vec![
    ///     Column::new("id", ColumnDataType::Integer, Some(true)), // Primary key column
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]).unwrap();
    ///
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string()]).unwrap();
    /// table.insert(vec!["2".to_string(), "Bob".to_string(), "30".to_string()]).unwrap();
    ///
    /// table.show();
    /// ```
    pub fn show(&self) {
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

        // Get the maximum number of rows across all columns
        let max_rows = self
            .columns
            .iter()
            .map(|column| column.data.len())
            .max()
            .unwrap_or(0);

        // Print the data rows
        for row_idx in 0..max_rows {
            for (_col_idx, column) in self.columns.iter().enumerate() {
                if row_idx < column.data.len() {
                    let value = &column.data[row_idx];
                    let padded_value = format!("{:<width$}", value, width = max_column_name_len);
                    print!("{} ", padded_value);
                } else {
                    let padding = " ".repeat(max_column_name_len);
                    print!("{} ", padding);
                }
            }
            println!();
        }
    }

    /// Displays the requested columns from the table.
    ///
    /// # Arguments
    ///
    /// * `column_names` - A vector of strings representing the names of the columns to display. If an empty vector is provided, the function will call the `show` function to display all columns.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the projection operation is successful.
    /// * `Err(Error)` if an error occurs during the projection operation.
    ///
    /// # Errors
    ///
    /// This function can return the following error:
    ///
    /// * `Error::NonExistingColumns` - If one or more of the provided column names do not exist in the table.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::table::Table;
    ///
    /// let mut table = Table::new("users", /* ... */);
    /// // Display all columns
    /// table.project(vec![]).unwrap();
    /// // Display specific columns
    /// table.project(vec!["user_id".to_string(), "age".to_string()]).unwrap();
    /// ```
    pub fn project(&self, column_names: Vec<String>) -> Result<(), Error> {
        if column_names.is_empty() {
            // If no column names are provided, call the show function
            self.show();
            return Ok(());
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

        // Find the maximum length of requested column names
        let max_column_name_len = column_names
            .iter()
            .map(|name| name.len())
            .max()
            .unwrap_or(0);

        // Print the requested column names
        for column_name in &column_names {
            let padded_name = format!("{:>width$}", column_name, width = max_column_name_len);
            print!("{} ", padded_name);
        }
        println!();

        // Print a separator line
        let separator_line: String = std::iter::repeat("-")
            .take(max_column_name_len * column_names.len() + column_names.len() - 1)
            .collect();
        println!("{}", separator_line);

        // Get the maximum number of rows across the requested columns
        let max_rows = column_names
            .iter()
            .map(|name| {
                self.columns
                    .iter()
                    .find(|c| c.name == *name)
                    .map(|c| c.data.len())
                    .unwrap_or(0)
            })
            .max()
            .unwrap_or(0);

        // Print the data rows for the requested columns
        for row_idx in 0..max_rows {
            for column_name in &column_names {
                if let Some(column) = self.columns.iter().find(|c| c.name == *column_name) {
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
            }
            println!();
        }

        Ok(())
    }

    /// Prints the structure of the table, including the column names, their corresponding data types, and primary key information.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let table = Table::new("users", vec![
    ///     Column::new("id", ColumnDataType::Integer, Some(true)), // Primary key column
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]).unwrap();
    ///
    /// table.describe();
    /// ```
    pub fn describe(&self) {
        println!("Table: {}", self.name);
        println!();

        // Find the maximum length of column names
        let max_column_name_len = self
            .columns
            .iter()
            .map(|column| column.name.len())
            .max()
            .unwrap_or(0);

        // Print the column names
        for column in &self.columns {
            let padded_name = format!("{:<width$}", column.name, width = max_column_name_len);
            print!("{} ", padded_name);
        }
        println!();

        // Print a separator line
        let separator_line: String = std::iter::repeat("-")
            .take(max_column_name_len * self.columns.len() + self.columns.len() - 1)
            .collect();
        println!("{}", separator_line);

        // Print the data types
        for column in &self.columns {
            let data_type_name = format!("{}", column.data_type);
            let padded_data_type =
                format!("{:<width$}", data_type_name, width = max_column_name_len);
            print!("{} ", padded_data_type);
        }
        println!();

        // Print primary key information
        for column in &self.columns {
            let primary_key_info = if column.is_primary_key {
                "prim_key".to_string()
            } else {
                "nt_prim_key".to_string()
            };
            let padded_primary_key_info =
                format!("{:<width$}", primary_key_info, width = max_column_name_len);
            print!("{} ", padded_primary_key_info);
        }
        println!();
    }

    /// Counts the number of records or non-null values in a specific column or the entire table.
    ///
    /// # Arguments
    ///
    /// * `column_name` - An optional string representing the name of the column to count non-null values for. If `None`, the function will count the total number of records in the table.
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - The count of records or non-null values, depending on whether a column name was provided or not.
    /// * `Err(Error)` - An error if the provided column name does not exist in the table.
    ///
    /// # Errors
    ///
    /// This function can return the following error:
    ///
    /// * `Error::NonExistingColumn` - If the provided column name does not exist in the table.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let mut table = Table::new("users", vec![
    ///     Column::new("id", ColumnDataType::Integer, Some(true)), // Primary key column
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]).unwrap();
    ///
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string()]).unwrap();
    /// table.insert(vec!["2".to_string(), "Bob".to_string(), "30".to_string()]).unwrap();
    /// table.insert(vec!["3".to_string(), "Charlie".to_string(), "".to_string()]).unwrap(); // null age
    ///
    /// // Count total records
    /// let total_count = table.count(None).unwrap();
    ///
    /// // Count non-null values in "age" column
    /// let age_count = table.count(Some("age".to_string())).unwrap();
    /// ```
    pub fn column_count(&self, column_name: Option<String>) -> Result<usize, Error> {
        return if let Some(column_name) = column_name {
            // Check if the provided column name exists
            if let Some(column) = self.columns.iter().find(|c| c.name == column_name) {
                // Count the non-null values in the specified column
                let non_null_count = column
                    .data
                    .iter()
                    .filter(|v| !matches!(v, Value::Null))
                    .count();
                Ok(non_null_count)
            } else {
                Err(Error::NonExistingColumn(column_name))
            }
        } else {
            // If no column name is provided, count the total number of records
            let max_rows = self
                .columns
                .iter()
                .map(|column| column.data.len())
                .max()
                .unwrap_or(0);
            Ok(max_rows)
        };
    }
}
