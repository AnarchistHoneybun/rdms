use crate::column::{Column, ColumnDataType};
use crate::table::{errors::Error, Table};

#[test]
fn test_project() {
    let mut table = Table::new(
        "test_table",
        vec![
            Column::new("id", ColumnDataType::Integer, None, true),
            Column::new("name", ColumnDataType::Text, None, false),
            Column::new("score", ColumnDataType::Float, None, false),
        ],
    )
    .unwrap();

    // Insert some initial data
    table
        .insert(vec![
            "1".to_string(),
            "Alice".to_string(),
            "85.5".to_string(),
        ])
        .unwrap();
    table
        .insert(vec!["2".to_string(), "Bob".to_string(), "92.0".to_string()])
        .unwrap();
    table
        .insert(vec![
            "3".to_string(),
            "Charlie".to_string(),
            "75.0".to_string(),
        ])
        .unwrap();

    // Test selecting all columns
    let result = table.project(vec![]);
    assert!(result.is_ok());

    // Test selecting specific columns
    let result = table.project(vec!["id".to_string(), "score".to_string()]);
    assert!(result.is_ok());

    // Test selecting non-existing columns
    let result = table.project(vec!["invalid".to_string()]);
    assert!(matches!(result, Err(Error::NonExistingColumns(_))));
}

#[test]
fn test_column_count() {
    let mut table = Table::new(
        "test_table",
        vec![
            Column::new("id", ColumnDataType::Integer, None, true),
            Column::new("name", ColumnDataType::Text, None, false),
            Column::new("score", ColumnDataType::Float, None, false),
        ],
    )
    .unwrap();

    // Insert some initial data
    table
        .insert(vec![
            "1".to_string(),
            "Alice".to_string(),
            "85.5".to_string(),
        ])
        .unwrap();
    table
        .insert(vec!["2".to_string(), "Bob".to_string(), "92.0".to_string()])
        .unwrap();
    table
        .insert(vec![
            "3".to_string(),
            "Charlie".to_string(),
            "75.0".to_string(),
        ])
        .unwrap();
    table
        .insert(vec![
            "4".to_string(),
            "NULL".to_string(),
            "NULL".to_string(),
        ])
        .unwrap(); // Insert a record with null values

    // Test counting total records
    let total_records = table.column_count(None).unwrap();
    assert_eq!(total_records, 4);

    // Test counting non-null values in a column
    let non_null_scores = table.column_count(Some("score".to_string())).unwrap();
    assert_eq!(non_null_scores, 3);

    // Test counting for a non-existing column
    let result = table.column_count(Some("invalid".to_string()));
    assert!(matches!(result, Err(Error::NonExistingColumn(_))));
}

#[test]
fn test_copy() {
    let mut original_table = Table::new(
        "test_table",
        vec![
            Column::new("id", ColumnDataType::Integer, None, true),
            Column::new("name", ColumnDataType::Text, None, false),
            Column::new("score", ColumnDataType::Float, None, false),
        ],
    )
    .unwrap();

    // Insert some initial data
    original_table
        .insert(vec![
            "1".to_string(),
            "Alice".to_string(),
            "85.5".to_string(),
        ])
        .unwrap();
    original_table
        .insert(vec!["2".to_string(), "Bob".to_string(), "92.0".to_string()])
        .unwrap();
    original_table
        .insert(vec![
            "3".to_string(),
            "Charlie".to_string(),
            "75.0".to_string(),
        ])
        .unwrap();

    // Create a copy of the table
    let copied_table = original_table.copy();

    // Check if the copied table has the same columns and data
    assert_eq!(copied_table.name, original_table.name);
    assert_eq!(copied_table.columns.len(), original_table.columns.len());

    for (original_column, copied_column) in original_table
        .columns
        .iter()
        .zip(copied_table.columns.iter())
    {
        assert_eq!(original_column.name, copied_column.name);
        assert_eq!(original_column.data_type, copied_column.data_type);
        assert_eq!(original_column.data, copied_column.data);
    }

    // Modify the original table and check if the copied table remains unchanged
    original_table
        .insert(vec![
            "4".to_string(),
            "Dave".to_string(),
            "68.0".to_string(),
        ])
        .unwrap();

    assert_ne!(
        original_table.columns[0].data.len(),
        copied_table.columns[0].data.len()
    );
}
