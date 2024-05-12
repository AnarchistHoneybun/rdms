use crate::column::{Column, ColumnDataType};
use crate::table::{Error, NestedCondition, Table};

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
