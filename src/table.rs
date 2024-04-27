use crate::column::{Column, ColumnDataType, Value};
use std::fmt;

#[derive(Debug)]
pub enum Error {
    MismatchedColumnCount,
    ParseError(usize, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MismatchedColumnCount => write!(f, "Number of values doesn't match the number of columns"),
            Error::ParseError(index, value) => write!(f, "Failed to parse value '{}' at index {}", value, index),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
}

impl Table {
    pub fn new(table_name: &str, columns: Vec<Column>) -> Table {
        Table {
            name: table_name.to_string(),
            columns,
        }
    }

    pub fn insert(&mut self, data: Vec<String>) -> Result<(), Error> {
        if data.len() != self.columns.len() {
            return Err(Error::MismatchedColumnCount);
        }

        let mut parsed_values: Vec<Value> = Vec::with_capacity(self.columns.len());

        for (column, value_str) in self.columns.iter().zip(data.into_iter()) {
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

        for (column, value) in self.columns.iter_mut().zip(parsed_values.into_iter()) {
            column.data.push(value);
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
            let padded_name = format!("{:<width$}", column.name, width = max_column_name_len);
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
            for (col_idx, column) in self.columns.iter().enumerate() {
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
}
