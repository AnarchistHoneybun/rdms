use crate::column::{Column, ColumnDataType, Value};
use std::{fmt, collections::HashSet};

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
    InvalidOperator(String), // operator_str
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MismatchedColumnCount => write!(f, "Number of values doesn't match the number of columns"),
            Error::ParseError(index, value) => write!(f, "Failed to parse value '{}' at index {}", value, index),
            Error::NonExistingColumns(columns) => write!(f, "The following columns do not exist: {}", columns.join(", ")),
            Error::NonExistingColumn(column_name) => write!(f, "The column '{}' does not exist", column_name),
            Error::InvalidOperator(operator_str) => write!(f, "Invalid operator: {}", operator_str),
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

    pub fn insert_with_columns(&mut self, column_names: Vec<String>, data: Vec<String>) -> Result<(), Error> {
        // Check if all provided column names exist in the table
        let column_names_set: HashSet<String> = column_names.iter().cloned().collect();
        let existing_columns: HashSet<String> = self.columns.iter().map(|c| c.name.clone()).collect();
        let non_existing_columns: Vec<String> = column_names_set.difference(&existing_columns).cloned().collect();

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

    pub fn update(&mut self, update_input: (String, String), condition_input: Option<(String, String, String)>) -> Result<(), Error> {
        // Validate column name in update_input
        let update_column = self.columns.iter().find(|c| c.name == update_input.0).ok_or(Error::NonExistingColumn(update_input.0.clone()))?;

        // Parse new_value according to the column's data type
        let new_value = match update_column.data_type {
            ColumnDataType::Integer => update_input.1.parse::<i64>().map(Value::Integer).map_err(|_| Error::ParseError(1, update_input.1.clone()))?,
            ColumnDataType::Float => update_input.1.parse::<f64>().map(Value::Float).map_err(|_| Error::ParseError(1, update_input.1.clone()))?,
            ColumnDataType::Text => Value::Text(update_input.1),
        };

        let update_column_name = self.columns.iter().find(|c| c.name == update_input.0).ok_or(Error::NonExistingColumn(update_input.0.clone()))?.name.clone();


        if let Some((cond_column_name, cond_value, operator_str)) = condition_input {

            // Validate condition column name
            let cond_column = self.columns.iter().find(|c| c.name == cond_column_name).ok_or(Error::NonExistingColumn(cond_column_name))?;

            // Parse the operator
            let operator = Operator::from_str(&operator_str).map_err(|e| Error::InvalidOperator(operator_str))?;


            // Update records based on the condition
            for record in &mut self.columns {
                if record.name == update_column_name {
                    record.data = record.data.iter().enumerate().filter_map(|(i, value)| {
                        if satisfies_condition(value, cond_column, &cond_value, &operator) {
                            Some(new_value.clone())
                        } else {
                            Some(value.clone())
                        }
                    }).collect();
                }
            }
        } else {
            // Update all records with the new_value
            for record in &mut self.columns {
                if record.name == update_input.0 {
                    record.data = vec![new_value.clone(); record.data.len()];
                }
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

    pub fn select(&self, column_names: Vec<String>) -> Result<(), Error> {
        if column_names.is_empty() {
            // If no column names are provided, call the show function
            self.show();
            return Ok(());
        }

        // Check if all provided column names exist in the table
        let column_names_set: HashSet<String> = column_names.iter().cloned().collect();
        let existing_columns: HashSet<String> = self.columns.iter().map(|c| c.name.clone()).collect();
        let non_existing_columns: Vec<String> = column_names_set.difference(&existing_columns).cloned().collect();

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
                        let padded_value = format!("{:>width$}", value, width = max_column_name_len);
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
            let padded_data_type = format!("{:<width$}", data_type_name, width = max_column_name_len);
            print!("{} ", padded_data_type);
        }
        println!();
    }
}

fn satisfies_condition(value: &Value, cond_column: &Column, cond_value: &str, operator: &Operator) -> bool {
    match (value, &cond_column.data_type) {
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
