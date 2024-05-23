use crate::column::{Column, ColumnDataType, ForeignKeyInfo};
use crate::table::NestedCondition;

mod column;
mod table;

mod database;

fn main() {
    let mut db = database::Database::new("my_db".to_string());

    let columns = vec![
        Column::new("id", ColumnDataType::Integer, None, true, None),
        Column::new("user_name", ColumnDataType::Text, None, false, None),
        Column::new("age", ColumnDataType::Integer, None, false, None),
    ];

    db.create_table("users", columns).unwrap();

    let users_data = vec![
        vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
        vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    ];
    for data in &users_data {
        if let Err(err) = db.insert_into_table("users", data.clone()) {
            eprintln!("Error inserting data: {}", err);
        }
    }

    // Print the table
    if let Some(table) = db.get_table("users") {
        table.show();
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

    // add some data to addresses table
    let addresses_data = vec![
        vec!["3".to_string(), "123 Main St.".to_string()],
        vec!["4".to_string(), "456 Elm St.".to_string()],
        vec!["2".to_string(), "789 Maple St.".to_string()],
    ];
    for data in &addresses_data {
        if let Err(err) = db.insert_into_table("addresses", data.clone()) {
            eprintln!("Error inserting data: {}", err);
        }
    }

    if let Err(err) = db.insert_with_columns_into_table(
        "addresses",
        vec!["address".to_string()],
        vec!["999 Oak St.".to_string()],
    ) {
        eprintln!("Error inserting data: {}", err);
    }

    // Print the table
    if let Some(table) = db.get_table("addresses") {
        table.show();
    }

    if let Err(err) = db.update_column_in_table("addresses", "user_id", "7") {
        eprintln!("Error updating column: {}", err);
    }

    // Update the "address" column with "999 Oak St." for records where "user_id" is 3
    let nested_condition =
        NestedCondition::Condition("user_id".to_string(), "=".to_string(), "3".to_string());
    db.update_with_nested_conditions_in_table(
        "addresses",
        ("address".to_string(), "999 Oak St.".to_string()),
        nested_condition,
    )
    .unwrap();

    // Print the table
    if let Some(table) = db.get_table("addresses") {
        table.show();
    }
}
