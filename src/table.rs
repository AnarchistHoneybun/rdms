use crate::column::{Column, ColumnDataType, Value};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::{collections::HashSet, fmt};

#[derive(Debug, PartialEq)]
enum Operator {
    Equal,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl Operator {
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

#[derive(Debug)]
pub struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

impl Table {
    pub fn new(table_name: &str, columns: Vec<Column>) -> Table {
        Table {
            name: table_name.to_string(),
            columns,
        }
    }

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
                    Some(header_line) => header_line
                        .split(',')
                        .map(|s| s.to_string())
                        .collect(),
                    None => return Err(Error::InvalidFormat("File is empty".to_string())),
                };

                // Read the column data types
                let column_data_types: Vec<ColumnDataType> = match lines.next() {
                    Some(data_types_line) => data_types_line
                        .split(',')
                        .map(|s| Ok(match s {
                            "Integer" => ColumnDataType::Integer,
                            "Float" => ColumnDataType::Float,
                            "Text" => ColumnDataType::Text,
                            _ => return Err(Error::InvalidFormat(format!("Invalid data type: {}", s))),
                        }))
                        .collect::<Result<Vec<_>, _>>()?,
                    None => return Err(Error::InvalidFormat("File is missing data types".to_string())),
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
                                    Err(_) => return Err(Error::ParseError(column.data.len(), value_str)),
                                },
                                ColumnDataType::Float => match value_str.parse::<f64>() {
                                    Ok(value) => column.data.push(Value::Float(value)),
                                    Err(_) => return Err(Error::ParseError(column.data.len(), value_str)),
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
                        .map(|s| Ok(match s {
                            "Integer" => ColumnDataType::Integer,
                            "Float" => ColumnDataType::Float,
                            "Text" => ColumnDataType::Text,
                            _ => return Err(Error::InvalidFormat(format!("Invalid data type: {}", s))),
                        }))
                        .collect::<Result<Vec<_>, _>>()?,
                    None => return Err(Error::InvalidFormat("File is missing data types".to_string())),
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
                    let row_values: Vec<String> = line
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect();
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
                                    Err(_) => return Err(Error::ParseError(column.data.len(), value_str)),
                                },
                                ColumnDataType::Float => match value_str.parse::<f64>() {
                                    Ok(value) => column.data.push(Value::Float(value)),
                                    Err(_) => return Err(Error::ParseError(column.data.len(), value_str)),
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
}

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
