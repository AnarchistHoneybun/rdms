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

    let mut users_table = Table::new("users", columns);

    users_table.show();
    print!("\n\n");

    let all_data = vec![
        vec!["1".to_string(), "Alice".to_string(), "27".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "35".to_string()],
        vec!["3".to_string(), "Joe".to_string(), "19".to_string()],
        vec!["6".to_string(),"Maleficent".to_string(),"invalid".to_string()],
        vec!["4".to_string(), "NULL".to_string(), "17".to_string()],
        vec!["5".to_string(), "Steve".to_string(), "40".to_string()],
    ];

    for data in all_data {
        if let Err(err) = users_table.insert(data.clone()) {
            println!("Error inserting data: {}", err);
        }
    }
    users_table.show();
    print!("\n\n");

    let column_names = vec!["user_id".to_string(), "age".to_string()];
    let data = vec!["6".to_string(), "31".to_string()];

    if let Err(err) = users_table.insert_with_columns(column_names, data) {
        println!("Error inserting data: {}", err);
    }

    users_table.show();
    print!("\n\n");

    let column_names = vec![
        "user_id".to_string(),
        "age".to_string(),
    ];

    if let Err(err) = users_table.select(column_names) {
        println!("Error selecting data: {}", err);
    }

    if let Err(err) = users_table.select(vec![]) {
        println!("Error selecting data: {}", err);
    }


    users_table.describe();
}
