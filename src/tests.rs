#[cfg(test)]
mod tests {
    use crate::column::{Column, ColumnDataType};
    use crate::table::{Error, Table};

    #[test]
    fn test_insert() {
        let mut table = Table::new(
            "test_table",
            vec![
                Column::new("id", ColumnDataType::Integer, None),
                Column::new("name", ColumnDataType::Text, None),
                Column::new("score", ColumnDataType::Float, None),
            ],
        );

        // Test inserting a valid record
        let result = table.insert(vec!["1".to_string(), "Alice".to_string(), "85.5".to_string()]);
        assert!(result.is_ok());

        // Test inserting a record with mismatched column count
        let result = table.insert(vec!["2".to_string(), "Bob".to_string()]);
        assert!(matches!(result, Err(Error::MismatchedColumnCount)));

        // Test inserting a record with invalid data type
        let result = table.insert(vec!["3".to_string(), "Charlie".to_string(), "invalid".to_string()]);
        assert!(matches!(result, Err(Error::ParseError(2, _))));
    }

    #[test]
    fn test_update() {
        let mut table = Table::new(
            "test_table",
            vec![
                Column::new("id", ColumnDataType::Integer, None),
                Column::new("name", ColumnDataType::Text, None),
                Column::new("score", ColumnDataType::Float, None),
            ],
        );

        // Insert some initial data
        table.insert(vec!["1".to_string(), "Alice".to_string(), "85.5".to_string()]).unwrap();
        table.insert(vec!["2".to_string(), "Bob".to_string(), "92.0".to_string()]).unwrap();
        table.insert(vec!["3".to_string(), "Charlie".to_string(), "75.0".to_string()]).unwrap();

        // Test updating a record without a condition
        let result = table.update(("score".to_string(), "100.0".to_string()), None);
        assert!(result.is_ok());

        // Test updating a record with a condition
        let result = table.update(
            ("name".to_string(), "Dave".to_string()),
            Some(("id".to_string(), "2".to_string(), "=".to_string())),
        );
        assert!(result.is_ok());

        // Test updating with a non-existing column
        let result = table.update(("invalid".to_string(), "value".to_string()), None);
        assert!(matches!(result, Err(Error::NonExistingColumn(_))));

        // Test updating with an invalid operator
        let result = table.update(
            ("score".to_string(), "80.0".to_string()),
            Some(("id".to_string(), "2".to_string(), "invalid".to_string())),
        );
        assert!(matches!(result, Err(Error::InvalidOperator(_))));
    }

    #[test]
    fn test_insert_with_columns() {
        let mut table = Table::new(
            "test_table",
            vec![
                Column::new("id", ColumnDataType::Integer, None),
                Column::new("name", ColumnDataType::Text, None),
                Column::new("score", ColumnDataType::Float, None),
            ],
        );

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
    fn test_select() {
        let mut table = Table::new(
            "test_table",
            vec![
                Column::new("id", ColumnDataType::Integer, None),
                Column::new("name", ColumnDataType::Text, None),
                Column::new("score", ColumnDataType::Float, None),
            ],
        );

        // Insert some initial data
        table
            .insert(vec!["1".to_string(), "Alice".to_string(), "85.5".to_string()])
            .unwrap();
        table
            .insert(vec!["2".to_string(), "Bob".to_string(), "92.0".to_string()])
            .unwrap();
        table
            .insert(vec!["3".to_string(), "Charlie".to_string(), "75.0".to_string()])
            .unwrap();

        // Test selecting all columns
        let result = table.select(vec![]);
        assert!(result.is_ok());

        // Test selecting specific columns
        let result = table.select(vec!["id".to_string(), "score".to_string()]);
        assert!(result.is_ok());

        // Test selecting non-existing columns
        let result = table.select(vec!["invalid".to_string()]);
        assert!(matches!(result, Err(Error::NonExistingColumns(_))));
    }
}