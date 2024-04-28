use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum ColumnDataType {
    Integer,
    Float,
    Text,
}

impl fmt::Display for ColumnDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnDataType::Integer => write!(f, "Integer"),
            ColumnDataType::Float => write!(f, "Float"),
            ColumnDataType::Text => write!(f, "Text"),
            // Add other data types as needed
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Text(String),
    Null,
}

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

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: ColumnDataType,
    pub data: Vec<Value>,
}

impl Column {
    pub fn new(name: &str, data_type: ColumnDataType, default_value: Option<Value>) -> Self {
        Column {
            name: name.to_owned(),
            data_type,
            data: match default_value {
                Some(value) => vec![value],
                None => Vec::new(),
            },
        }
    }
}
