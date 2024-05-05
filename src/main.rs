use crate::column::{Column, ColumnDataType};
use crate::table::{NestedCondition, Table};

mod column;
mod table;
mod tests;

fn main() {
    // 1. Create a new table
    let columns = vec![
        Column::new("user_id", ColumnDataType::Integer, None),
        Column::new("user_name", ColumnDataType::Text, None),
        Column::new("age", ColumnDataType::Integer, None),
    ];

    let mut users_table = Table::new("users", columns);
    println!("Created a new table 'users':");
    users_table.describe();
    print!("\n\n");

    // 2. Insert data into the table
    let all_data = vec![
        vec!["1".to_string(), "Alice".to_string(), "27".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "35".to_string()],
        vec!["3".to_string(), "Joe".to_string(), "19".to_string()],
        vec![
            "6".to_string(),
            "Maleficent".to_string(),
            "invalid".to_string(),
        ],
        vec!["4".to_string(), "NULL".to_string(), "17".to_string()],
        vec!["5".to_string(), "Steve".to_string(), "40".to_string()],
    ];

    println!("Inserting data into the table:");
    for data in &all_data {
        if let Err(err) = users_table.insert(data.clone()) {
            println!("Error inserting data: {}", err);
        }
    }
    users_table.show();
    print!("\n\n");

    // 3. Insert data into specific columns
    let column_names = vec!["user_id".to_string(), "age".to_string()];
    let data = vec!["6".to_string(), "31".to_string()];

    println!("Inserting data into specific columns:");
    if let Err(err) = users_table.insert_with_columns(column_names, data) {
        println!("Error inserting data: {}", err);
    }
    users_table.show();
    print!("\n\n");

    // 4. Project specific columns
    let column_names = vec!["user_id".to_string(), "age".to_string()];

    println!("Projecting specific columns:");
    if let Err(err) = users_table.project(column_names.clone()) {
        println!("Error selecting data: {}", err);
    }
    print!("\n\n");

    // 5. Project all columns
    println!("Projecting all columns:");
    if let Err(err) = users_table.project(vec![]) {
        println!("Error selecting data: {}", err);
    }
    print!("\n\n");

    // 6. Update a column with a new value
    println!("Updating the 'age' column with the value '30':");
    if let Err(err) = users_table.update_column("age", "30") {
        println!("Error updating data: {}", err);
    }
    users_table.show();
    print!("\n\n");

    // 7. Count records
    let total_records = users_table.count(None);
    if let Err(err) = total_records {
        println!("Error counting records: {}", err);
    } else {
        println!("Total records: {}", total_records.unwrap());
    }

    let non_null_names = users_table.count(Some("user_name".to_string()));
    if let Err(err) = non_null_names {
        println!("Error counting records: {}", err);
    } else {
        println!("Total non-null names: {}", non_null_names.unwrap());
    }
    print!("\n\n");

    // 8. Export table to CSV
    println!("Exporting table to CSV file 'users.csv':");
    users_table.export_table("users.csv", "csv").unwrap();
    print!("\n\n");

    // 9. Copy table
    println!("Creating a copy of the table:");
    let _users_copy = users_table.copy();
    print!("\n\n");

    // 10. Import table from file
    println!("Importing table from file 'users.txt':");
    match Table::import_table("users.txt", "txt") {
        Ok(table) => {
            println!("Table imported successfully");
            table.show();
        }
        Err(err) => {
            eprintln!("Error: {}", err);
        }
    }
    print!("\n\n");

    // 11. Update table with nested conditions
    println!("Updating table with nested conditions:");
    let nested_condition = NestedCondition::And(
        Box::new(NestedCondition::Condition(
            "age".to_string(),
            "=".to_string(),
            "30".to_string(),
        )),
        Box::new(NestedCondition::Or(
            Box::new(NestedCondition::Condition(
                "user_id".to_string(),
                "=".to_string(),
                "4".to_string(),
            )),
            Box::new(NestedCondition::Condition(
                "user_id".to_string(),
                "=".to_string(),
                "5".to_string(),
            )),
        )),
    );

    if let Err(err) = users_table.update_with_nested_conditions(
        ("user_name".to_string(), "Sam".to_string()),
        nested_condition,
    ) {
        println!("Error updating data: {}", err);
    }
    users_table.show();

    // Example usage
    let nested_condition = NestedCondition::And(
        Box::new(NestedCondition::Condition(
            "age".to_string(),
            ">=".to_string(),
            "25".to_string(),
        )),
        Box::new(NestedCondition::Condition(
            "user_name".to_string(),
            "!=".to_string(),
            "Alice".to_string(),
        )),
    );

    users_table
        .filter_with_nested_conditions(nested_condition)
        .unwrap();

    // 12. Filter and project table
    println!("Filtering and projecting table:");
    let column_names = vec!["user_id".to_string()];
    let nested_condition = NestedCondition::And(
        Box::new(NestedCondition::Condition(
            "age".to_string(),
            ">=".to_string(),
            "30".to_string(),
        )),
        Box::new(NestedCondition::Condition(
            "user_name".to_string(),
            "=".to_string(),
            "Sam".to_string(),
        )),
    );

    if let Err(err) = users_table.filter_and_project(column_names, nested_condition) {
        println!("Error filtering and projecting data: {}", err);
    }
    print!("\n\n");
}
