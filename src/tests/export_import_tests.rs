use crate::column::{ColumnDataType, Value};
use crate::table::{Error, Table};

#[test]
fn test_import_table() {
    // Test importing a CSV file
    let result = Table::import_table("test_files/data/test_data.csv", "csv");
    match result {
        Ok(table) => {
            assert_eq!(table.name, "test_files/data/test_data.csv");
            assert_eq!(table.columns.len(), 3);

            assert_eq!(table.columns[0].name, "id");
            assert_eq!(table.columns[0].data_type, ColumnDataType::Integer);
            assert_eq!(
                table.columns[0].data,
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
            );

            assert_eq!(table.columns[1].name, "name");
            assert_eq!(table.columns[1].data_type, ColumnDataType::Text);
            assert_eq!(
                table.columns[1].data,
                vec![
                    Value::Text("Alice".to_string()),
                    Value::Text("Bob".to_string()),
                    Value::Text("Charlie".to_string())
                ]
            );

            assert_eq!(table.columns[2].name, "score");
            assert_eq!(table.columns[2].data_type, ColumnDataType::Float);
            assert_eq!(
                table.columns[2].data,
                vec![Value::Float(85.5), Value::Float(92.0), Value::Float(75.0)]
            );
        }
        Err(err) => panic!("Error importing CSV file: {}", err),
    }

    // Test importing a TXT file
    let result = Table::import_table("test_files/data/test_data.txt", "txt");
    match result {
        Ok(table) => {
            assert_eq!(table.name, "test_files/data/test_data.txt");
            assert_eq!(table.columns.len(), 3);

            assert_eq!(table.columns[0].name, "id");
            assert_eq!(table.columns[0].data_type, ColumnDataType::Integer);
            assert_eq!(
                table.columns[0].data,
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
            );

            assert_eq!(table.columns[1].name, "name");
            assert_eq!(table.columns[1].data_type, ColumnDataType::Text);
            assert_eq!(
                table.columns[1].data,
                vec![
                    Value::Text("Alice".to_string()),
                    Value::Text("Bob".to_string()),
                    Value::Text("Charlie".to_string())
                ]
            );

            assert_eq!(table.columns[2].name, "score");
            assert_eq!(table.columns[2].data_type, ColumnDataType::Float);
            assert_eq!(
                table.columns[2].data,
                vec![Value::Float(85.5), Value::Float(92.0), Value::Float(75.0)]
            );
        }
        Err(err) => panic!("Error importing TXT file: {}", err),
    }

    // Test importing a non-existing file
    let result = Table::import_table("test_files/data/non_existing.csv", "csv");
    assert!(matches!(result, Err(Error::FileError(_))));

    // Test importing a file with invalid format
    let result = Table::import_table("test_files/data/test_data.txt", "pdf");
    assert!(matches!(result, Err(Error::InvalidFormat(_))));
}
