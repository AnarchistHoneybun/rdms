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
    fn test_update_column() {
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

        // Test updating a column with a valid value
        let result = table.update_column("score", "100.0");
        assert!(result.is_ok());

        // Test updating a non-existing column
        let result = table.update_column("invalid", "value");
        assert!(matches!(result, Err(Error::NonExistingColumn(_))));

        // Test updating with an invalid value for the column data type
        let result = table.update_column("id", "invalid");
        assert!(matches!(result, Err(Error::ParseError(0, _))));
    }

    #[test]
    fn test_update_with_conditions() {
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

        // Test updating with a single condition
        let result = table.update_with_conditions(
            ("score".to_string(),
            "100.0".to_string(),),
            vec![("id".to_string(), "2".to_string(), "=".to_string())],
            "and",
        );
        assert!(result.is_ok());

        // Test updating with multiple conditions (AND logic)
        let result = table.update_with_conditions(
            ("name".to_string(),
            "Dave".to_string()),
            vec![
                ("id".to_string(), "2".to_string(), "=".to_string()),
                ("score".to_string(), "92.0".to_string(), "=".to_string()),
            ],
            "and",
        );
        assert!(result.is_ok());

        // Test updating with multiple conditions (OR logic)
        let result = table.update_with_conditions(
            ("score".to_string(),
            "80.0".to_string()),
            vec![
                ("id".to_string(), "1".to_string(), "=".to_string()),
                ("id".to_string(), "3".to_string(), "=".to_string()),
            ],
            "or",
        );
        assert!(result.is_ok());

        // Test updating with a non-existing column in the condition
        let result = table.update_with_conditions(
            ("score".to_string(),
            "90.0".to_string()),
            vec![("invalid".to_string(), "value".to_string(), "=".to_string())],
            "and",
        );
        assert!(matches!(result, Err(Error::NonExistingColumn(_))));

        // Test updating with an invalid operator in the condition
        let result = table.update_with_conditions(
            ("score".to_string(),
            "90.0".to_string()),
            vec![("id".to_string(), "2".to_string(), "invalid".to_string())],
            "and",
        );
        assert!(matches!(result, Err(Error::InvalidOperator(_))));

        // Test updating with an invalid logic string
        let result = table.update_with_conditions(
            ("score".to_string(),
            "90.0".to_string()),
            vec![("id".to_string(), "2".to_string(), "=".to_string())],
            "invalid",
        );
        assert!(matches!(result, Err(Error::InvalidLogic(_))));
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

    #[test]
    fn test_count() {
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
        table.insert(vec!["4".to_string(), "NULL".to_string(), "NULL".to_string()]).unwrap(); // Insert a record with null values

        // Test counting total records
        let total_records = table.count(None).unwrap();
        assert_eq!(total_records, 4);

        // Test counting non-null values in a column
        let non_null_scores = table.count(Some("score".to_string())).unwrap();
        assert_eq!(non_null_scores, 3);

        // Test counting for a non-existing column
        let result = table.count(Some("invalid".to_string()));
        assert!(matches!(result, Err(Error::NonExistingColumn(_))));
    }

    #[test]
    fn test_copy() {
        let mut original_table = Table::new(
            "test_table",
            vec![
                Column::new("id", ColumnDataType::Integer, None),
                Column::new("name", ColumnDataType::Text, None),
                Column::new("score", ColumnDataType::Float, None),
            ],
        );

        // Insert some initial data
        original_table
            .insert(vec!["1".to_string(), "Alice".to_string(), "85.5".to_string()])
            .unwrap();
        original_table
            .insert(vec!["2".to_string(), "Bob".to_string(), "92.0".to_string()])
            .unwrap();
        original_table
            .insert(vec!["3".to_string(), "Charlie".to_string(), "75.0".to_string()])
            .unwrap();

        // Create a copy of the table
        let copied_table = original_table.copy();

        // Check if the copied table has the same columns and data
        assert_eq!(copied_table.name, original_table.name);
        assert_eq!(copied_table.columns.len(), original_table.columns.len());

        for (original_column, copied_column) in original_table.columns.iter().zip(copied_table.columns.iter()) {
            assert_eq!(original_column.name, copied_column.name);
            assert_eq!(original_column.data_type, copied_column.data_type);
            assert_eq!(original_column.data, copied_column.data);
        }

        // Modify the original table and check if the copied table remains unchanged
        original_table
            .insert(vec!["4".to_string(), "Dave".to_string(), "68.0".to_string()])
            .unwrap();

        assert_ne!(original_table.columns[0].data.len(), copied_table.columns[0].data.len());
    }
}