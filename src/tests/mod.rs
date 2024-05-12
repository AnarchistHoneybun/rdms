mod export_import_tests;

use crate::column::{Column, ColumnDataType};
use crate::table::{Error, NestedCondition, Table};

#[test]
fn test_insert() {
    let mut table = Table::new(
        "test_table",
        vec![
            Column::new("id", ColumnDataType::Integer, None, true),
            Column::new("name", ColumnDataType::Text, None, false),
            Column::new("score", ColumnDataType::Float, None, false),
        ],
    )
    .unwrap();

    // Test inserting a valid record
    let result = table.insert(vec![
        "1".to_string(),
        "Alice".to_string(),
        "85.5".to_string(),
    ]);
    assert!(result.is_ok());

    // Test inserting a record with mismatched column count
    let result = table.insert(vec!["2".to_string(), "Bob".to_string()]);
    assert!(matches!(result, Err(Error::MismatchedColumnCount)));

    // Test inserting a record with invalid data type
    let result = table.insert(vec![
        "3".to_string(),
        "Charlie".to_string(),
        "invalid".to_string(),
    ]);
    assert!(matches!(result, Err(Error::ParseError(2, _))));
}

#[test]
fn test_update_column() {
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

    // Test updating a column with a valid value
    let result = table.update_column("score", "100.0");
    assert!(result.is_ok());

    // Test updating a non-existing column
    let result = table.update_column("invalid", "value");
    assert!(matches!(result, Err(Error::NonExistingColumn(_))));

    // Test updating with an invalid value for the column data type
    let result = table.update_column("score", "invalid");
    assert!(matches!(result, Err(Error::ParseError(0, _))));
}

#[test]
fn test_update_with_nested_conditions() {
    let mut table = Table::new(
        "test_table",
        vec![
            Column::new("user_id", ColumnDataType::Integer, None, true),
            Column::new("user_name", ColumnDataType::Text, None, false),
            Column::new("age", ColumnDataType::Integer, None, false),
        ],
    )
    .unwrap();

    // Insert some initial data
    table
        .insert(vec!["1".to_string(), "Alice".to_string(), "27".to_string()])
        .unwrap();
    table
        .insert(vec!["2".to_string(), "Bob".to_string(), "35".to_string()])
        .unwrap();
    table
        .insert(vec![
            "3".to_string(),
            "Charlie".to_string(),
            "19".to_string(),
        ])
        .unwrap();
    table
        .insert(vec!["4".to_string(), "Dave".to_string(), "30".to_string()])
        .unwrap();
    table
        .insert(vec!["5".to_string(), "Eve".to_string(), "30".to_string()])
        .unwrap();

    // Test updating with a single condition
    let nested_condition =
        NestedCondition::Condition("age".to_string(), "=".to_string(), "30".to_string());
    let result = table.update_with_nested_conditions(
        ("user_name".to_string(), "Sam".to_string()),
        nested_condition,
    );
    assert!(result.is_ok());

    // Test updating with nested conditions (AND and OR)
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
    let result = table.update_with_nested_conditions(
        ("user_name".to_string(), "Sam".to_string()),
        nested_condition,
    );
    assert!(result.is_ok());

    // Test updating with a non-existing column in the condition
    let nested_condition =
        NestedCondition::Condition("invalid".to_string(), "=".to_string(), "value".to_string());
    let result = table.update_with_nested_conditions(
        ("user_name".to_string(), "Sam".to_string()),
        nested_condition,
    );
    assert!(matches!(result, Err(Error::NonExistingColumn(_))));

    // Test updating with an invalid operator in the condition
    let nested_condition =
        NestedCondition::Condition("age".to_string(), "invalid".to_string(), "30".to_string());
    let result = table.update_with_nested_conditions(
        ("user_name".to_string(), "Sam".to_string()),
        nested_condition,
    );
    assert!(matches!(result, Err(Error::InvalidOperator(_))));
}

#[test]
fn test_insert_with_columns() {
    let mut table = Table::new(
        "test_table",
        vec![
            Column::new("id", ColumnDataType::Integer, None, true),
            Column::new("name", ColumnDataType::Text, None, false),
            Column::new("score", ColumnDataType::Float, None, false),
        ],
    )
    .unwrap();

    // Test inserting a valid record
    let result = table.insert_with_columns(
        vec!["id".to_string(), "name".to_string()],
        vec!["1".to_string(), "Alice".to_string()],
    );
    assert!(result.is_ok());

    // Test inserting a record with non-existing columns
    let result = table.insert_with_columns(
        vec!["id".to_string(), "invalid".to_string()],
        vec!["2".to_string(), "value".to_string()],
    );
    assert!(matches!(result, Err(Error::NonExistingColumns(_))));

    // Test inserting a record with mismatched column count
    let result = table.insert_with_columns(
        vec!["id".to_string(), "name".to_string(), "score".to_string()],
        vec!["3".to_string(), "Charlie".to_string()],
    );
    assert!(matches!(result, Err(Error::MismatchedColumnCount)));

    // Test inserting a record with invalid data type
    let result = table.insert_with_columns(
        vec!["id".to_string(), "score".to_string()],
        vec!["4".to_string(), "invalid".to_string()],
    );
    assert!(matches!(
        result,
        Err(Error::ParseError(_, value_str)) if value_str == "invalid"
    ));
}

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
fn test_count() {
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

#[test]
fn test_filter_with_nested_conditions() {
    let mut table = Table::new(
        "test_table",
        vec![
            Column::new("id", ColumnDataType::Integer, None, false),
            Column::new("name", ColumnDataType::Text, None, false),
            Column::new("age", ColumnDataType::Integer, None, false),
            Column::new("score", ColumnDataType::Float, None, false),
        ],
    )
    .unwrap();

    // Insert some initial data
    table
        .insert(vec![
            "1".to_string(),
            "Alice".to_string(),
            "25".to_string(),
            "85.5".to_string(),
        ])
        .unwrap();
    table
        .insert(vec![
            "2".to_string(),
            "Bob".to_string(),
            "30".to_string(),
            "92.0".to_string(),
        ])
        .unwrap();
    table
        .insert(vec![
            "3".to_string(),
            "Charlie".to_string(),
            "35".to_string(),
            "75.0".to_string(),
        ])
        .unwrap();
    table
        .insert(vec![
            "4".to_string(),
            "Dave".to_string(),
            "28".to_string(),
            "88.0".to_string(),
        ])
        .unwrap();

    // Test filtering with a single condition
    let nested_condition =
        NestedCondition::Condition("age".to_string(), "=".to_string(), "30".to_string());
    let result = table.filter_with_nested_conditions(nested_condition);
    assert!(result.is_ok());

    // Test filtering with nested conditions (AND and OR)
    let nested_condition = NestedCondition::And(
        Box::new(NestedCondition::Condition(
            "age".to_string(),
            ">".to_string(),
            "25".to_string(),
        )),
        Box::new(NestedCondition::Or(
            Box::new(NestedCondition::Condition(
                "score".to_string(),
                ">=".to_string(),
                "85.0".to_string(),
            )),
            Box::new(NestedCondition::Condition(
                "name".to_string(),
                "=".to_string(),
                "Charlie".to_string(),
            )),
        )),
    );
    let result = table.filter_with_nested_conditions(nested_condition);
    assert!(result.is_ok());

    // Test filtering with a non-existing column in the condition
    let nested_condition =
        NestedCondition::Condition("invalid".to_string(), "=".to_string(), "value".to_string());
    let result = table.filter_with_nested_conditions(nested_condition);
    assert!(matches!(result, Err(Error::NonExistingColumn(_))));

    // Test filtering with an invalid operator in the condition
    let nested_condition =
        NestedCondition::Condition("age".to_string(), "invalid".to_string(), "30".to_string());
    let result = table.filter_with_nested_conditions(nested_condition);
    assert!(matches!(result, Err(Error::InvalidOperator(_))));
}

#[test]
fn test_filter_and_project() {
    let mut table = Table::new(
        "test_table",
        vec![
            Column::new("id", ColumnDataType::Integer, None, true),
            Column::new("name", ColumnDataType::Text, None, false),
            Column::new("age", ColumnDataType::Integer, None, false),
            Column::new("score", ColumnDataType::Float, None, false),
        ],
    )
    .unwrap();

    // Insert some initial data
    table
        .insert(vec![
            "1".to_string(),
            "Alice".to_string(),
            "25".to_string(),
            "85.5".to_string(),
        ])
        .unwrap();
    table
        .insert(vec![
            "2".to_string(),
            "Bob".to_string(),
            "30".to_string(),
            "92.0".to_string(),
        ])
        .unwrap();
    table
        .insert(vec![
            "3".to_string(),
            "Charlie".to_string(),
            "35".to_string(),
            "75.0".to_string(),
        ])
        .unwrap();
    table
        .insert(vec![
            "4".to_string(),
            "Dave".to_string(),
            "28".to_string(),
            "88.0".to_string(),
        ])
        .unwrap();

    // Filter and project with a single condition
    let nested_condition =
        NestedCondition::Condition("age".to_string(), ">".to_string(), "25".to_string());
    let result =
        table.filter_and_project(vec!["id".to_string(), "name".to_string()], nested_condition);
    assert!(result.is_ok());

    // Filter and project with nested conditions (AND and OR)
    let nested_condition = NestedCondition::And(
        Box::new(NestedCondition::Condition(
            "age".to_string(),
            ">".to_string(),
            "25".to_string(),
        )),
        Box::new(NestedCondition::Or(
            Box::new(NestedCondition::Condition(
                "score".to_string(),
                ">=".to_string(),
                "85.0".to_string(),
            )),
            Box::new(NestedCondition::Condition(
                "name".to_string(),
                "=".to_string(),
                "Charlie".to_string(),
            )),
        )),
    );
    let result = table.filter_and_project(
        vec!["id".to_string(), "name".to_string(), "score".to_string()],
        nested_condition,
    );
    assert!(result.is_ok());

    // Filter and project with non-existing columns
    let nested_condition =
        NestedCondition::Condition("age".to_string(), ">".to_string(), "25".to_string());
    let result = table.filter_and_project(vec!["invalid".to_string()], nested_condition);
    assert!(matches!(result, Err(Error::NonExistingColumns(_))));

    // Filter and project with a non-existing column in the condition
    let nested_condition =
        NestedCondition::Condition("invalid".to_string(), "=".to_string(), "value".to_string());
    let result =
        table.filter_and_project(vec!["id".to_string(), "name".to_string()], nested_condition);
    assert!(matches!(result, Err(Error::NonExistingColumn(_))));

    // Filter and project with an invalid operator in the condition
    let nested_condition =
        NestedCondition::Condition("age".to_string(), "invalid".to_string(), "30".to_string());
    let result =
        table.filter_and_project(vec!["id".to_string(), "name".to_string()], nested_condition);
    assert!(matches!(result, Err(Error::InvalidOperator(_))));
}
