use crate::column::{Column, ColumnDataType, Value};
use crate::table::{Error, NestedCondition, Operator};

/// Evaluates a nested condition structure against a specific row in the table.
///
/// # Arguments
///
/// * `condition` - A reference to the `NestedCondition` enum representing the nested condition structure.
/// * `columns` - A slice of `Column` instances representing the columns in the table.
/// * `row_idx` - The index of the row to evaluate the conditions against.
///
/// # Returns
///
/// * `Ok(bool)` - `true` if the row satisfies the nested condition, `false` otherwise.
/// * `Err(Error)` - An error if a column in the condition does not exist in the table or if an invalid operator is used.
///
/// # Errors
///
/// This function can return the following errors:
///
/// * `Error::NonExistingColumn` - If a column in the condition does not exist in the table.
/// * `Error::InvalidOperator` - If an invalid operator is used in the condition.
pub(crate) fn evaluate_nested_conditions(
    condition: &NestedCondition,
    columns: &[Column],
    row_idx: usize,
) -> Result<bool, Error> {
    match condition {
        NestedCondition::Condition(column_name, operator, value) => {
            let cond_column_data_type = columns
                .iter()
                .find(|c| c.name == *column_name)
                .ok_or(Error::NonExistingColumn(column_name.clone()))?
                .data_type
                .clone();

            let operator = Operator::from_str(&operator)
                .map_err(|_e| Error::InvalidOperator(operator.clone()))?;

            let ref_value = columns
                .iter()
                .find(|c| c.name == *column_name)
                .ok_or(Error::NonExistingColumn(column_name.clone()))?
                .data
                .get(row_idx)
                .cloned();

            Ok(ref_value.map_or(false, |v| {
                satisfies_condition(&v, cond_column_data_type, &value, &operator)
            }))
        }
        NestedCondition::And(left, right) => {
            let left_result = evaluate_nested_conditions(left, columns, row_idx)?;
            let right_result = evaluate_nested_conditions(right, columns, row_idx)?;
            Ok(left_result && right_result)
        }
        NestedCondition::Or(left, right) => {
            let left_result = evaluate_nested_conditions(left, columns, row_idx)?;
            let right_result = evaluate_nested_conditions(right, columns, row_idx)?;
            Ok(left_result || right_result)
        }
    }
}

/// Checks if a value satisfies a specific condition based on the provided operator and condition value.
///
/// # Arguments
///
/// * `value` - A reference to the `Value` enum representing the value to check.
/// * `cond_column_data_type` - The `ColumnDataType` of the column the condition is based on.
/// * `cond_value` - A string slice representing the condition value.
/// * `operator` - A reference to the `Operator` enum representing the comparison operator.
///
/// # Returns
///
/// * `bool` - `true` if the value satisfies the condition, `false` otherwise.
pub fn satisfies_condition(
    value: &Value,
    cond_column_data_type: ColumnDataType,
    cond_value: &str,
    operator: &Operator,
) -> bool {
    match (value, &cond_column_data_type) {
        (Value::Integer(val), ColumnDataType::Integer) => {
            let cond_value: i64 = cond_value.parse().unwrap();
            match operator {
                Operator::Equal => val == &cond_value,
                Operator::NotEqual => val != &cond_value,
                Operator::LessThan => val < &cond_value,
                Operator::GreaterThan => val > &cond_value,
                Operator::LessThanOrEqual => val <= &cond_value,
                Operator::GreaterThanOrEqual => val >= &cond_value,
            }
        }
        (Value::Float(val), ColumnDataType::Float) => {
            let cond_value: f64 = cond_value.parse().unwrap();
            match operator {
                Operator::Equal => val == &cond_value,
                Operator::NotEqual => val != &cond_value,
                Operator::LessThan => val < &cond_value,
                Operator::GreaterThan => val > &cond_value,
                Operator::LessThanOrEqual => val <= &cond_value,
                Operator::GreaterThanOrEqual => val >= &cond_value,
            }
        }
        (Value::Text(val), ColumnDataType::Text) => match operator {
            Operator::Equal => val == cond_value,
            Operator::NotEqual => val != cond_value,
            _ => false, // Other operators not supported for Text data type
        },
        _ => false, // Unsupported data type or value combination
    }
}
