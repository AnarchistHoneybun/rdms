use std::fmt;

/// Supported datatypes for columns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnDataType {
    Integer,
    Float,
    Text,
}

/// Implement the Display trait for ColumnDataType,
/// for printing in table description/error messages/table export
impl fmt::Display for ColumnDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnDataType::Integer => write!(f, "Integer"),
            ColumnDataType::Float => write!(f, "Float"),
            ColumnDataType::Text => write!(f, "Text"),
        }
    }
}

/// Supported value types for columns.
/// Distinct from datatype as this is actual data storage and that is more metadata-ish.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Text(String),
    Null,
}

/// Implement the Display trait for Value,
/// for printing in select functions/table export/table display
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(value) => write!(
                f,
                "{:>width$}",
                value,
                width = f.width().unwrap_or_default()
            ),
            Value::Float(value) => write!(
                f,
                "{:>width$.2}",
                value,
                width = f.width().unwrap_or_default()
            ),
            Value::Text(value) => write!(
                f,
                "{:>width$}",
                value,
                width = f.width().unwrap_or_default()
            ),
            Value::Null => write!(
                f,
                "{:>width$}",
                "NULL",
                width = f.width().unwrap_or_default()
            ),
            // Add other variants as needed
        }
    }
}

#[derive(Debug, Clone)]
pub struct ForeignKeyInfo {
    pub reference_table: String,
    pub reference_column: String,
}

/// Struct used to store columns.
/// Each column has a name, datatype and a vector of values of type ColumnDataType.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: ColumnDataType,
    pub data: Vec<Value>,
    pub is_primary_key: bool,
    pub foreign_key: Option<ForeignKeyInfo>,
}

impl Column {
    /// Create a new column with the given name, datatype and default value.
    pub fn new(
        name: &str,
        data_type: ColumnDataType,
        default_value: Option<Value>,
        is_primary_key: bool,
        foreign_key: Option<ForeignKeyInfo>,
    ) -> Self {
        Column {
            name: name.to_owned(),
            data_type,
            data: match default_value {
                Some(value) => vec![value],
                None => Vec::new(),
            },
            is_primary_key,
            foreign_key,
        }
    }
}
