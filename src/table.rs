use crate::column::{Column, Value};

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

    pub fn insert(&mut self, data: Vec<Value>) -> Result<(), &'static str> {
        if data.len() != self.columns.len() {
            panic!("Data length does not match column length");
        }

        for (column, value) in self.columns.iter_mut().zip(data.into_iter()) {
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
