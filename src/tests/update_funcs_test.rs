use crate::column::{Column, ColumnDataType};
use crate::table::{Error, NestedCondition, Table};

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