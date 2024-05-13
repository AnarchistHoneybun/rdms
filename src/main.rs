use crate::column::{Column, ColumnDataType, ForeignKeyInfo};

mod column;
mod table;

mod database;
#[cfg(test)]
mod tests;

fn main() {
    let mut db = database::Database::new("my_db".to_string());

    let columns = vec![
        Column::new("id", ColumnDataType::Integer, None, true, None),
        Column::new("user_name", ColumnDataType::Text, None, false, None),
        Column::new("age", ColumnDataType::Integer, None, false, None),
    ];

    db.create_table("users", columns).unwrap();

    if let Some(users_table) = db.get_table_mut("users") {
        let users_data = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
            vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
        ];
        for data in &users_data {
            if let Err(err) = users_table.insert(data.clone()) {
                eprintln!("Error inserting data: {}", err);
            }
        }

        // Print the table
        users_table.show();
    }

    let columns = vec![
        Column::new(
            "user_id",
            ColumnDataType::Integer,
            None,
            false,
            ForeignKeyInfo::new("users", "id").into(),
        ),
        Column::new("address", ColumnDataType::Text, None, false, None),
    ];

    db.create_table("addresses", columns).unwrap();

    if let Some(address_table) = db.get_table_mut("addresses") {
        // add some data to addresses table
        let addresses_data = vec![
            vec!["3".to_string(), "123 Main St.".to_string()],
            vec!["1".to_string(), "456 Elm St.".to_string()],
            vec!["2".to_string(), "789 Maple St.".to_string()],
        ];
        for data in &addresses_data {
            if let Err(err) = address_table.insert(data.clone()) {
                eprintln!("Error inserting data: {}", err);
            }
        }

        // Print the table
        address_table.show();
    }
}
