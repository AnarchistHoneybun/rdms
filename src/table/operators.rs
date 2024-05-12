/// This Operator enum represents the different comparison operators that can be used in an update
/// or select condition. These are mapped to respective operations on execution.
#[derive(Debug, PartialEq)]
pub(crate) enum Operator {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl Operator {
    /// This function converts a string to an Operator enum. It returns an error if the requested string
    /// is not a supported operator.
    pub(crate) fn from_str(s: &str) -> Result<Operator, String> {
        match s {
            "=" => Ok(Operator::Equal),
            "!=" => Ok(Operator::NotEqual),
            "<" => Ok(Operator::LessThan),
            ">" => Ok(Operator::GreaterThan),
            "<=" => Ok(Operator::LessThanOrEqual),
            ">=" => Ok(Operator::GreaterThanOrEqual),
            _ => Err(format!("Invalid operator: {}", s)),
        }
    }
}