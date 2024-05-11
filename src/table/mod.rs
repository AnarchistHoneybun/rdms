mod export_import;
mod helpers;

use helpers::evaluate_nested_conditions;

use crate::column::{Column, ColumnDataType, Value};
use std::io::{BufRead, Write};
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
    CannotBatchUpdatePrimaryKey,
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
            Error::CannotBatchUpdatePrimaryKey => {
                write!(f, "Primary key column disallows batch updates")
            }
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
