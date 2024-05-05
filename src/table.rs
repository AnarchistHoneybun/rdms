use crate::column::{Column, ColumnDataType, Value};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::{collections::HashSet, fmt};

#[derive(Debug)]
pub enum NestedCondition {
    Condition(String, String, String),
    And(Box<NestedCondition>, Box<NestedCondition>),
    Or(Box<NestedCondition>, Box<NestedCondition>),
}

/// This Operator enum represents the different comparison operators that can be used in an update
/// or select condition. These are mapped to respective operations on execution.
#[derive(Debug, PartialEq)]
enum Operator {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl Operator {
    /// This function converts a string to an Operator enum. It returns an error if the requested string
    /// is not a supported operator.
    fn from_str(s: &str) -> Result<Operator, String> {
        match s {
            "=" => Ok(Operator::Equal),
            "!=" => Ok(Operator::NotEqual),
            "<" => Ok(Operator::LessThan),
            ">" => Ok(Operator::GreaterThan),
            "<=" => Ok(Operator::LessThanOrEqual),
            ">=" => Ok(Operator::GreaterThanOrEqual),
            _ => Err(format!("Invalid operator: {}", s)),
        }
    }
}

/// Enum for various error types that can occur during table operations.
#[derive(Debug)]
pub enum Error {
    MismatchedColumnCount,
    ParseError(usize, String),
    NonExistingColumns(Vec<String>),
    NonExistingColumn(String), // column_name
    InvalidOperator(String),   // operator_str
    FileError(String),
    InvalidFormat(String),
    MultiplePrimaryKeys,
    DuplicatePrimaryKey,
    NullPrimaryKey,
}

/// Implement Display trait for Error enum to allow for custom error messages.
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MismatchedColumnCount => {
                write!(f, "Number of values doesn't match the number of columns")
            }
            Error::ParseError(index, value) => {
                write!(f, "Failed to parse value '{}' at index {}", value, index)
            }
            Error::NonExistingColumns(columns) => write!(
                f,
                "The following columns do not exist: {}",
                columns.join(", ")
            ),
            Error::NonExistingColumn(column_name) => {
                write!(f, "The column '{}' does not exist", column_name)
            }
            Error::InvalidOperator(operator_str) => write!(f, "Invalid operator: {}", operator_str),
            Error::FileError(msg) => write!(f, "File error: {}", msg),
            Error::InvalidFormat(format) => write!(f, "Invalid format: {}", format),
            Error::MultiplePrimaryKeys => write!(f, "Multiple primary keys are not allowed"),
            Error::DuplicatePrimaryKey => write!(f, "Duplicate primary key value"),
            Error::NullPrimaryKey => write!(f, "Primary key value cannot be null"),
        }
    }
}

impl std::error::Error for Error {}

/// Struct representing a table with a name and a vector of columns
/// (data is stored inside the column struct).
#[derive(Debug)]
pub struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) primary_key_column: Option<Column>,
}

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
    /// A `Table` instance with the provided name and columns.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let columns = vec![
    ///     Column::new("id", ColumnDataType::Integer, None),
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ];
    ///
    /// let table = Table::new("users", columns);
    /// ```
    pub fn new(table_name: &str, mut columns: Vec<Column>) -> Result<Table, Error> {
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
    /// A `Table` instance with the same name and columns as the current instance, but with a deep copy of the data.
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
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string()]).unwrap();
    ///
    /// let table_copy = table.copy();
    /// ```
    pub fn copy(&self) -> Table {
        let mut new_columns = Vec::with_capacity(self.columns.len());
        let mut new_primary_key_column: Option<Column> = None;

        for column in &self.columns {
            let mut new_column = Column::new(
                &*column.name.clone(),
                column.data_type.clone(),
                None,
                column.is_primary_key,
            );
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

        let mut parsed_values: Vec<Value> = vec![Value::Null; self.columns.len()];

        for (column_name, value_str) in column_names.iter().zip(data.into_iter()) {
            if let Some(column_idx) = self.columns.iter().position(|c| c.name == *column_name) {
                let column = &self.columns[column_idx];
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

        for (column, value) in self.columns.iter_mut().zip(parsed_values.into_iter()) {
            column.data.push(value);
        }

        Ok(())
    }

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
            }
        }

        Ok(())
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
    ///     Column::new("id", ColumnDataType::Integer, None),
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]);
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

    /// Function to display only requested columns from the table (if called with an empty column
    /// list, will call the show function).
    ///
    /// # Arguments
    ///
    /// * `column_names` - A vector of strings representing the names of the columns to display.
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

    /// Prints the structure of the table, including the column names and their corresponding data types.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::column::{Column, ColumnDataType};
    /// use crate::table::Table;
    ///
    /// let table = Table::new("users", vec![
    ///     Column::new("id", ColumnDataType::Integer, None),
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]);
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
    ///     Column::new("id", ColumnDataType::Integer, None),
    ///     Column::new("name", ColumnDataType::Text, None),
    ///     Column::new("age", ColumnDataType::Integer, None),
    /// ]);
    ///
    /// // Insert some initial data
    /// table.insert(vec!["1".to_string(), "Alice".to_string(), "25".to_string()]).unwrap();
    /// table.insert(vec!["2".to_string(), "Bob".to_string(), "30".to_string()]).unwrap();
    /// table.insert(vec!["3".to_string(), "Charlie".to_string(), "".to_string()]).unwrap(); // null age
    ///
    /// // Count total records
    /// let total_count = table.count(None).unwrap();
    /// assert_eq!(total_count, 3);
    ///
    /// // Count non-null values in "age" column
    /// let age_count = table.count(Some("age".to_string())).unwrap();
    /// assert_eq!(age_count, 2);
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

    /// Function to export the table to a CSV or TXT file based on input.
    ///
    /// # Arguments
    ///
    /// * `file_name` - A string representing the name of the file to export.
    /// * `format` - A string representing the format of the file, either "csv" or "txt".
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the export operation is successful.
    /// * `Err(Error)` if an error occurs during the export operation.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `Error::FileError` - If the file fails to create or write.
    /// * `Error::InvalidFormat` - If the provided format is not "csv" or "txt".
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::table::Table;
    ///
    /// let table = Table::new("users", /* ... */);
    /// // Export to CSV
    /// table.export_table("users.csv", "csv").unwrap();
    /// // Export to TXT
    /// table.export_table("users.txt", "txt").unwrap();
    /// ```
    pub fn export_table(&self, file_name: &str, format: &str) -> Result<(), Error> {
        let path = Path::new(file_name);
        let file = match File::create(path) {
            Ok(file) => file,
            Err(e) => return Err(Error::FileError(format!("Failed to create file: {}", e))),
        };
        let mut writer = BufWriter::new(file);

        match format.to_lowercase().as_str() {
            "csv" => {
                // Write column names as header
                let header = self
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                writer
                    .write_all(header.as_bytes())
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                writer
                    .write_all(b"\n")
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;

                // Write column data types
                let data_types = self
                    .columns
                    .iter()
                    .map(|c| format!("{}", c.data_type))
                    .collect::<Vec<_>>()
                    .join(",");
                writer
                    .write_all(data_types.as_bytes())
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                writer
                    .write_all(b"\n")
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;

                // Write data rows
                let max_rows = self
                    .columns
                    .iter()
                    .map(|column| column.data.len())
                    .max()
                    .unwrap_or(0);

                for row_idx in 0..max_rows {
                    let row_data: Vec<String> = self
                        .columns
                        .iter()
                        .map(|column| {
                            if row_idx < column.data.len() {
                                format!("{}", column.data[row_idx])
                            } else {
                                "".to_string()
                            }
                        })
                        .collect();

                    let row_string = row_data.join(",");
                    writer
                        .write_all(row_string.as_bytes())
                        .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                    writer
                        .write_all(b"\n")
                        .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                }
            }
            "txt" => {
                // Find the maximum length of column names
                let max_column_name_len = self
                    .columns
                    .iter()
                    .map(|column| column.name.len())
                    .max()
                    .unwrap_or(0);

                // Print the column names
                for column in &self.columns {
                    let padded_name =
                        format!("{:>width$}", column.name, width = max_column_name_len);
                    writer
                        .write_all(padded_name.as_bytes())
                        .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                    writer
                        .write_all(b" ")
                        .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                }
                writer
                    .write_all(b"\n")
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;

                // Print the column data types
                for column in &self.columns {
                    let padded_data_type =
                        format!("{:<width$}", column.data_type, width = max_column_name_len);
                    writer
                        .write_all(padded_data_type.as_bytes())
                        .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                    writer
                        .write_all(b" ")
                        .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                }
                writer
                    .write_all(b"\n")
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;

                // Print a separator line
                let separator_line: String = std::iter::repeat("-")
                    .take(max_column_name_len * self.columns.len() + self.columns.len() - 1)
                    .collect();
                writer
                    .write_all(separator_line.as_bytes())
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                writer
                    .write_all(b"\n")
                    .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;

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
                            let padded_value =
                                format!("{:<width$}", value, width = max_column_name_len);
                            writer.write_all(padded_value.as_bytes()).map_err(|e| {
                                Error::FileError(format!("Failed to write to file: {}", e))
                            })?;
                            writer.write_all(b" ").map_err(|e| {
                                Error::FileError(format!("Failed to write to file: {}", e))
                            })?;
                        } else {
                            let padding = " ".repeat(max_column_name_len);
                            writer.write_all(padding.as_bytes()).map_err(|e| {
                                Error::FileError(format!("Failed to write to file: {}", e))
                            })?;
                            writer.write_all(b" ").map_err(|e| {
                                Error::FileError(format!("Failed to write to file: {}", e))
                            })?;
                        }
                    }
                    writer
                        .write_all(b"\n")
                        .map_err(|e| Error::FileError(format!("Failed to write to file: {}", e)))?;
                }
            }
            _ => return Err(Error::InvalidFormat(format.to_string())),
        }

        Ok(())
    }

    /// Imports a table stored in CSV or TXT format and defines a table variable from it.
    /// Only reads data that is stored in the same format as exported by the export function.
    ///
    /// # Arguments
    ///
    /// * `file_name` - A string representing the name of the file to import.
    /// * `format` - A string representing the format of the file, either "csv" or "txt".
    ///
    /// # Returns
    ///
    /// * `Ok(Table)` - A `Table` instance created from the imported data if the import operation is successful.
    /// * `Err(Error)` - An `Error` if the import operation fails, e.g., file error, invalid format, or parsing error.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    ///
    /// * `Error::FileError` - If the file fails to open or read.
    /// * `Error::InvalidFormat` - If the provided format is not "csv" or "txt", or if the file has an invalid format.
    /// * `Error::MismatchedColumnCount` - If the number of values in a row does not match the number of columns.
    /// * `Error::ParseError` - If a value in the file cannot be parsed into the corresponding column's data type.
    /// # Examples
    ///
    /// ```
    /// use crate::table::Table;
    ///
    /// let table = Table::import_table("data.csv", "csv").unwrap();
    /// // or
    /// let table = Table::import_table("data.txt", "txt").unwrap();
    /// ```
    pub fn import_table(file_name: &str, format: &str) -> Result<Table, Error> {
        let path = Path::new(file_name);
        let file = match File::open(path) {
            Ok(file) => file,
            Err(e) => return Err(Error::FileError(format!("Failed to open file: {}", e))),
        };

        match format.to_lowercase().as_str() {
            "csv" => {
                let reader = BufReader::new(file);
                let mut lines = reader.lines().map(|line| line.unwrap());

                // Read the column names
                let column_names: Vec<String> = match lines.next() {
                    Some(header_line) => header_line.split(',').map(|s| s.to_string()).collect(),
                    None => return Err(Error::InvalidFormat("File is empty".to_string())),
                };

                // Read the column data types
                let column_data_types: Vec<ColumnDataType> = match lines.next() {
                    Some(data_types_line) => data_types_line
                        .split(',')
                        .map(|s| {
                            Ok(match s {
                                "Integer" => ColumnDataType::Integer,
                                "Float" => ColumnDataType::Float,
                                "Text" => ColumnDataType::Text,
                                _ => {
                                    return Err(Error::InvalidFormat(format!(
                                        "Invalid data type: {}",
                                        s
                                    )))
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    None => {
                        return Err(Error::InvalidFormat(
                            "File is missing data types".to_string(),
                        ))
                    }
                };

                // Create columns with the corresponding data types
                let mut columns: Vec<Column> = column_names
                    .iter()
                    .zip(column_data_types.iter())
                    .map(|(name, data_type)| Column::new(name, data_type.clone(), None))
                    .collect();

                // Read the data rows
                for line in lines {
                    let row_values: Vec<String> = line.split(',').map(|s| s.to_string()).collect();
                    if row_values.len() != column_names.len() {
                        return Err(Error::MismatchedColumnCount);
                    }

                    for (column, value_str) in columns.iter_mut().zip(row_values.into_iter()) {
                        if value_str.trim().to_lowercase() == "null" {
                            column.data.push(Value::Null);
                        } else {
                            match column.data_type {
                                ColumnDataType::Integer => match value_str.parse::<i64>() {
                                    Ok(value) => column.data.push(Value::Integer(value)),
                                    Err(_) => {
                                        return Err(Error::ParseError(column.data.len(), value_str))
                                    }
                                },
                                ColumnDataType::Float => match value_str.parse::<f64>() {
                                    Ok(value) => column.data.push(Value::Float(value)),
                                    Err(_) => {
                                        return Err(Error::ParseError(column.data.len(), value_str))
                                    }
                                },
                                ColumnDataType::Text => column.data.push(Value::Text(value_str)),
                            }
                        }
                    }
                }

                let table_name = file_name.to_string();
                Ok(Table::new(&table_name, columns))
            }
            "txt" => {
                let reader = BufReader::new(file);
                let mut lines = reader.lines().map(|line| line.unwrap());

                // Read the column names
                let column_names: Vec<String> = match lines.next() {
                    Some(header_line) => header_line
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect(),
                    None => return Err(Error::InvalidFormat("File is empty".to_string())),
                };

                // Read the column data types
                let column_data_types: Vec<ColumnDataType> = match lines.next() {
                    Some(data_types_line) => data_types_line
                        .split_whitespace()
                        .map(|s| {
                            Ok(match s {
                                "Integer" => ColumnDataType::Integer,
                                "Float" => ColumnDataType::Float,
                                "Text" => ColumnDataType::Text,
                                _ => {
                                    return Err(Error::InvalidFormat(format!(
                                        "Invalid data type: {}",
                                        s
                                    )))
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    None => {
                        return Err(Error::InvalidFormat(
                            "File is missing data types".to_string(),
                        ))
                    }
                };

                // Create columns with the corresponding data types
                let mut columns: Vec<Column> = column_names
                    .iter()
                    .zip(column_data_types.iter())
                    .map(|(name, data_type)| Column::new(name, data_type.clone(), None))
                    .collect();

                // the text file format has one line of seperators, so we need to skip it
                lines.next();

                // Read the data rows
                for line in lines {
                    let row_values: Vec<String> =
                        line.split_whitespace().map(|s| s.to_string()).collect();
                    if row_values.len() != column_names.len() {
                        return Err(Error::MismatchedColumnCount);
                    }

                    for (column, value_str) in columns.iter_mut().zip(row_values.into_iter()) {
                        if value_str.trim().to_lowercase() == "null" {
                            column.data.push(Value::Null);
                        } else {
                            match column.data_type {
                                ColumnDataType::Integer => match value_str.parse::<i64>() {
                                    Ok(value) => column.data.push(Value::Integer(value)),
                                    Err(_) => {
                                        return Err(Error::ParseError(column.data.len(), value_str))
                                    }
                                },
                                ColumnDataType::Float => match value_str.parse::<f64>() {
                                    Ok(value) => column.data.push(Value::Float(value)),
                                    Err(_) => {
                                        return Err(Error::ParseError(column.data.len(), value_str))
                                    }
                                },
                                ColumnDataType::Text => column.data.push(Value::Text(value_str)),
                            }
                        }
                    }
                }

                let table_name = file_name.to_string();
                Ok(Table::new(&table_name, columns))
            }
            _ => Err(Error::InvalidFormat(format.to_string())),
        }
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

/// Evaluates a nested condition structure against a specific row in the table.
///
/// # Arguments
///
/// * `condition` - A reference to the `NestedCondition` enum representing the nested condition structure.
/// * `columns` - A slice of `Column` instances representing the columns in the table.
/// * `row_idx` - The index of the row to evaluate the conditions against.
///
/// # Returns
///
/// * `Ok(bool)` - `true` if the row satisfies the nested condition, `false` otherwise.
/// * `Err(Error)` - An error if a column in the condition does not exist in the table or if an invalid operator is used.
///
/// # Errors
///
/// This function can return the following errors:
///
/// * `Error::NonExistingColumn` - If a column in the condition does not exist in the table.
/// * `Error::InvalidOperator` - If an invalid operator is used in the condition.
fn evaluate_nested_conditions(
    condition: &NestedCondition,
    columns: &[Column],
    row_idx: usize,
) -> Result<bool, Error> {
    match condition {
        NestedCondition::Condition(column_name, operator, value) => {
            let cond_column_data_type = columns
                .iter()
                .find(|c| c.name == *column_name)
                .ok_or(Error::NonExistingColumn(column_name.clone()))?
                .data_type
                .clone();

            let operator = Operator::from_str(&operator)
                .map_err(|_e| Error::InvalidOperator(operator.clone()))?;

            let ref_value = columns
                .iter()
                .find(|c| c.name == *column_name)
                .ok_or(Error::NonExistingColumn(column_name.clone()))?
                .data
                .get(row_idx)
                .cloned();

            Ok(ref_value.map_or(false, |v| {
                satisfies_condition(&v, cond_column_data_type, &value, &operator)
            }))
        }
        NestedCondition::And(left, right) => {
            let left_result = evaluate_nested_conditions(left, columns, row_idx)?;
            let right_result = evaluate_nested_conditions(right, columns, row_idx)?;
            Ok(left_result && right_result)
        }
        NestedCondition::Or(left, right) => {
            let left_result = evaluate_nested_conditions(left, columns, row_idx)?;
            let right_result = evaluate_nested_conditions(right, columns, row_idx)?;
            Ok(left_result || right_result)
        }
    }
}

/// Checks if a value satisfies a specific condition based on the provided operator and condition value.
///
/// # Arguments
///
/// * `value` - A reference to the `Value` enum representing the value to check.
/// * `cond_column_data_type` - The `ColumnDataType` of the column the condition is based on.
/// * `cond_value` - A string slice representing the condition value.
/// * `operator` - A reference to the `Operator` enum representing the comparison operator.
///
/// # Returns
///
/// * `bool` - `true` if the value satisfies the condition, `false` otherwise.
fn satisfies_condition(
    value: &Value,
    cond_column_data_type: ColumnDataType,
    cond_value: &str,
    operator: &Operator,
) -> bool {
    match (value, &cond_column_data_type) {
        (Value::Integer(val), ColumnDataType::Integer) => {
            let cond_value: i64 = cond_value.parse().unwrap();
            match operator {
                Operator::Equal => val == &cond_value,
                Operator::NotEqual => val != &cond_value,
                Operator::LessThan => val < &cond_value,
                Operator::GreaterThan => val > &cond_value,
                Operator::LessThanOrEqual => val <= &cond_value,
                Operator::GreaterThanOrEqual => val >= &cond_value,
            }
        }
        (Value::Float(val), ColumnDataType::Float) => {
            let cond_value: f64 = cond_value.parse().unwrap();
            match operator {
                Operator::Equal => val == &cond_value,
                Operator::NotEqual => val != &cond_value,
                Operator::LessThan => val < &cond_value,
                Operator::GreaterThan => val > &cond_value,
                Operator::LessThanOrEqual => val <= &cond_value,
                Operator::GreaterThanOrEqual => val >= &cond_value,
            }
        }
        (Value::Text(val), ColumnDataType::Text) => match operator {
            Operator::Equal => val == cond_value,
            Operator::NotEqual => val != cond_value,
            _ => false, // Other operators not supported for Text data type
        },
        _ => false, // Unsupported data type or value combination
    }
}
