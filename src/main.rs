use crate::column::{Column, ColumnDataType};
use crate::table::Table;

mod column;
mod table;

fn main() {
    let columns = vec![
        Column::new("user_id", ColumnDataType::Integer, None),
        Column::new("user_name", ColumnDataType::Text, None),
        Column::new("age", ColumnDataType::Integer, None),
    ];

    let mut table = Table::new("users", columns);

    table.show();
    print!("\n\n");

    let data = vec![
        "1".to_string(),
        "Alice".to_string(),
        "27".to_string(),
    ];

    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    table.show();
    print!("\n\n");

    let data = vec![
        "2".to_string(),
        "Bob".to_string(),
        "35".to_string(),
    ];

    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    let data = vec![
        "3".to_string(),
        "Joe".to_string(),
        "19".to_string(),
    ];

    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    table.show();
    print!("\n\n");

    let data = vec![
        "6".to_string(),
        "Maleficent".to_string(),
        "invalid".to_string(), // This will cause a parsing error
    ];

    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    let data = vec![
        "4".to_string(),
        "NULL".to_string(), // This will be treated as a null value
        "17".to_string(),
    ];

    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    let data = vec![
        "5".to_string(),
        "Steve".to_string(), // This will be treated as a null value
        "40".to_string(),
    ];

    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    table.show();
    print!("\n\n");

    table.describe();
}
