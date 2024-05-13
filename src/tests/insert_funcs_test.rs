use crate::column::{Column, ColumnDataType};
use crate::table::{errors::Error, Table};

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

    // Test inserting a record with null primary key
    let result = table.insert(vec![
        "null".to_string(),
        "Dave".to_string(),
        "75.0".to_string(),
    ]);
    assert!(matches!(result, Err(Error::NullPrimaryKey)));

    // Test inserting a record with duplicate primary key
    table
        .insert(vec!["5".to_string(), "Eve".to_string(), "92.0".to_string()])
        .unwrap();
    let result = table.insert(vec![
        "5".to_string(),
        "Frank".to_string(),
        "88.0".to_string(),
    ]);
    assert!(matches!(result, Err(Error::DuplicatePrimaryKey)));
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

    // Test inserting a record without providing the primary key column
    let result = table.insert_with_columns(
        vec!["name".to_string(), "score".to_string()],
        vec!["Bob".to_string(), "88.5".to_string()],
    );
    assert!(matches!(result, Err(Error::PrimaryKeyNotProvided(_))));

    // Test inserting a record with null primary key
    let result = table.insert_with_columns(
        vec!["id".to_string(), "name".to_string(), "score".to_string()],
        vec![
            "null".to_string(),
            "Charlie".to_string(),
            "75.0".to_string(),
        ],
    );
    assert!(matches!(result, Err(Error::NullPrimaryKey)));

    // Test inserting a record with duplicate primary key
    table
        .insert_with_columns(
            vec!["id".to_string(), "name".to_string()],
            vec!["5".to_string(), "Dave".to_string()],
        )
        .unwrap();
    let result = table.insert_with_columns(
        vec!["id".to_string(), "name".to_string(), "score".to_string()],
        vec!["5".to_string(), "Eve".to_string(), "92.0".to_string()],
    );
    assert!(matches!(result, Err(Error::DuplicatePrimaryKey)));
}
