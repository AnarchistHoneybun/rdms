mod column;
mod table;

use crate::column::{Column, ColumnDataType, Value};
use crate::table::Table;

fn main() {
    let columns = vec![
        Column::new("user_id", ColumnDataType::Integer, None),
        Column::new("user_name", ColumnDataType::Text, None),
        Column::new("age", ColumnDataType::Integer, None),
    ];

    let mut table = Table::new("users", columns);

    table.show();
    print!("\n\n\n");

    let data = vec![
        Value::Integer(1),
        Value::Text("Alice".to_string()),
        Value::Integer(30),
    ];

    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    table.show();
    print!("\n\n\n");

    let data = vec![
        Value::Integer(2),
        Value::Text("Bob".to_string()),
        Value::Integer(11),
    ];
    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    let data = vec![
        Value::Integer(3),
        Value::Text("Joe".to_string()),
        Value::Integer(25),
    ];
    if let Err(err) = table.insert(data) {
        println!("Error inserting data: {}", err);
    }

    table.show();
    print!("\n\n\n");
}
