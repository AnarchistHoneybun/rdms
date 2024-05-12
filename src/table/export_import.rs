use crate::column::{Column, ColumnDataType, Value};
use crate::table::Error;
use crate::table::Table;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

impl Table {
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

                // Write primary key information
                let primary_key_info = self
                    .columns
                    .iter()
                    .map(|c| {
                        if c.is_primary_key {
                            "prim_key".to_string()
                        } else {
                            "nt_prim_key".to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                writer
                    .write_all(primary_key_info.as_bytes())
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

                // Print primary key information
                for column in &self.columns {
                    let primary_key_info = if column.is_primary_key {
                        "prim_key".to_string()
                    } else {
                        "nt_prim_key".to_string()
                    };
                    let padded_primary_key_info =
                        format!("{:<width$}", primary_key_info, width = max_column_name_len);
                    writer
                        .write_all(padded_primary_key_info.as_bytes())
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

                // Read the primary key information
                let primary_key_info: Vec<bool> = match lines.next() {
                    Some(primary_key_line) => primary_key_line
                        .split(',')
                        .map(|s| {
                            Ok(match s {
                                "prim_key" => true,
                                "nt_prim_key" => false,
                                _ => {
                                    return Err(Error::InvalidFormat(format!(
                                        "Invalid primary key information: {}",
                                        s
                                    )))
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    None => {
                        return Err(Error::InvalidFormat(
                            "File is missing primary key information".to_string(),
                        ))
                    }
                };

                // Create columns with the corresponding data types and primary key information
                let mut columns: Vec<Column> = column_names
                    .iter()
                    .zip(column_data_types.iter())
                    .zip(primary_key_info.iter())
                    .map(|((name, data_type), is_primary_key)| {
                        Column::new(name, data_type.clone(), None, *is_primary_key)
                    })
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
                Table::new(&table_name, columns)
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

                // Read the primary key information
                let primary_key_info: Vec<bool> = match lines.next() {
                    Some(primary_key_line) => primary_key_line
                        .split_whitespace()
                        .map(|s| {
                            Ok(match s {
                                "prim_key" => true,
                                "nt_prim_key" => false,
                                _ => {
                                    return Err(Error::InvalidFormat(format!(
                                        "Invalid primary key information: {}",
                                        s
                                    )))
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    None => {
                        return Err(Error::InvalidFormat(
                            "File is missing primary key information".to_string(),
                        ))
                    }
                };

                // the text file format has one line of separators, so we need to skip it
                lines.next();

                // Create columns with the corresponding data types and primary key information
                let mut columns: Vec<Column> = column_names
                    .iter()
                    .zip(column_data_types.iter())
                    .zip(primary_key_info.iter())
                    .map(|((name, data_type), is_primary_key)| {
                        Column::new(name, data_type.clone(), None, *is_primary_key)
                    })
                    .collect();

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
                Table::new(&table_name, columns)
            }
            _ => Err(Error::InvalidFormat(format.to_string())),
        }
    }
}
