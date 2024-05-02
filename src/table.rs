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
    InvalidLogic(String),
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
            Error::InvalidLogic(logic) => write!(f, "Invalid logic: {}", logic),
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
}

impl Table {

    /// Function to create a new table, given a name and a vector of columns.
    pub fn new(table_name: &str, columns: Vec<Column>) -> Table {
        Table {
            name: table_name.to_string(),
            columns,
        }
    }

    /// Function to create a copy of the table. Useful when trying to create backups/perform
    /// simultaneous edits for comparison.
    pub fn copy(&self) -> Table {
        let mut new_columns = Vec::with_capacity(self.columns.len());

        for column in &self.columns {
            let mut new_column = Column::new(&*column.name.clone(), column.data_type.clone(), None);
            new_column.data = column.data.clone();
            new_columns.push(new_column);
        }

        Table {
            name: self.name.clone(),
            columns: new_columns,
        }
    }

    /// Function to insert new data into the table. This inserts a whole record at a time,
    /// therefore will error out if number of provided data points does not match the number of
    /// columns in the table.
    ///
    /// Function flow: accept data -> check if number of data points match number of columns (error
    /// out if not) -> parse data points (error out if data cannot be parsed
    /// into the data type of respective column) -> insert parsed data into table.
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

        for (column, value) in self.columns.iter_mut().zip(parsed_values.into_iter()) {
            column.data.push(value);
        }

        Ok(())
    }

    /// Function to insert a new record, but can be formatted to only insert data into specific columns.
    /// Will fill the other columns with a null value.
    ///
    /// Function flow: accept column names and data -> check if provided column names exist in the
    /// table (error out if not) -> check if number of data points match number of column names (error
    /// out if not) -> parse data points (error out if data cannot be parsed into the data type of
    /// the respective column) (data points are null on init so columns with no provided
    /// data end up null on insertion) -> insert parsed data into table.
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

    /// Function to update a particular column for all records.
    ///
    /// Function flow: accept column name and new value -> check if column exists in the table (error
    /// out if not) -> parse new value (error out if data cannot be parsed into the data type of
    /// provided column) -> update the entire column with the parsed data point.
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

    /// Function to update a particular column for all records, but can be formatted to only update
    /// records that meet a certain conditions in one or more columns (these can be the same as the column being updated
    /// or different). Can be conditional on meeting all conditions or any one of the conditions.
    ///
    /// Function flow: accept column name, new value, conditions, and logic ->
    /// check if column to be updated exists in the table (error out if not) -> parse new value (error
    /// out if it cannot be parsed into the data type of the requested column) ->
    /// iterate over all records, and for each column, check if that is the field that needs update
    /// -> evaluate all conditions for the record (error out if a condition is invalid based on
    /// operator, invalid column etc. or if provided logic string is not supported) -> update record
    /// if requisite conditions are met.
    pub fn update_with_conditions(
        &mut self,
        update_input: (String, String),
        conditions: Vec<(String, String, String)>,
        logic: &str,
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
                            evaluate_conditions(&columns_clone, &conditions, i, logic)?;

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

    /// Function to print the entire record to the terminal.
    ///
    /// Function flow: find the largest column name, so rest of the columns can be padded
    /// accordingly -> print the data out line by line, column by column.
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
    /// Function flow: check if column names are provided -> check if provided column names exist in
    /// the table (error out if not) -> find the maximum length of requested column names -> print
    /// requested columns out to the terminal.
    pub fn select(&self, column_names: Vec<String>) -> Result<(), Error> {
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

    /// Function to provide the structure of the table. Lists all the columns and their data types.
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

    /// Function to count the number of records in the table. Is able to accept a column name as
    /// input. If no column name is provided, returns the total record count. Otherwise, returns the total
    /// number of not-null values in that column for that table.
    pub fn count(&self, column_name: Option<String>) -> Result<usize, Error> {
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

    /// Function to export the table to a csv or txt file based on input.
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

    /// Function to import a table stored in csv or txt format and define a table variable from it.
    /// Only reads data that is stored in the same format as exported by the export function.
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
}

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

/// Function to check if a given column satisfies a particular condition based on
/// provided operator, reference value and the column it is conditional on. Will error out
/// if any of these are not properly formatted or are un-supported data types.
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
                Operator::LessThan => val < &cond_value,
                Operator::GreaterThan => val > &cond_value,
                Operator::LessThanOrEqual => val <= &cond_value,
                Operator::GreaterThanOrEqual => val >= &cond_value,
            }
        }
        (Value::Text(val), ColumnDataType::Text) => match operator {
            Operator::Equal => val == cond_value,
            _ => false, // Other operators not supported for Text data type
        },
        _ => false, // Unsupported data type or value combination
    }
}

/// Function to batch evaluate multiple conditions on a column, calls the satisfies_condition
/// function for all provided conditions and returns a flag.
fn evaluate_conditions(
    columns: &[Column],
    conditions: &[(String, String, String)],
    row_idx: usize,
    logic: &str,
) -> Result<bool, Error> {
    let mut update_record = if logic.eq_ignore_ascii_case("and") {
        true
    } else if logic.eq_ignore_ascii_case("or") {
        false
    } else {
        return Err(Error::InvalidLogic(logic.to_string()));
    };

    for (cond_column_name, cond_value, operator_str) in conditions {
        let cond_column_data_type = columns
            .iter()
            .find(|c| c.name == *cond_column_name)
            .ok_or(Error::NonExistingColumn(cond_column_name.clone()))?
            .data_type
            .clone();

        let operator = Operator::from_str(&operator_str)
            .map_err(|_e| Error::InvalidOperator(operator_str.clone()))?;

        let ref_value = columns
            .iter()
            .find(|c| c.name == *cond_column_name)
            .ok_or(Error::NonExistingColumn(cond_column_name.clone()))?
            .data
            .get(row_idx)
            .cloned();

        let condition_satisfied = ref_value.map_or(false, |v| {
            satisfies_condition(&v, cond_column_data_type, &cond_value, &operator)
        });

        if logic.eq_ignore_ascii_case("and") {
            update_record &= condition_satisfied;
        } else if logic.eq_ignore_ascii_case("or") {
            update_record |= condition_satisfied;
        } else {
            return Err(Error::InvalidLogic(logic.to_string()));
        }
    }

    Ok(update_record)
}
