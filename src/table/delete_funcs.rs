use crate::table::helpers::evaluate_nested_conditions;
use crate::table::{Error, NestedCondition, Table};

impl Table {
    pub fn delete_with_nested_conditions(
        &mut self,
        nested_condition: &NestedCondition,
    ) -> Result<(), Error> {
        let mut rows_to_remove = Vec::new();

        for row_idx in 0..self.columns[0].data.len() {
            if evaluate_nested_conditions(nested_condition, &self.columns, row_idx)? {
                rows_to_remove.push(row_idx);
            }
        }

        // dbg!(&rows_to_remove);

        // Remove rows from each column's data vector
        for col in &mut self.columns {
            for row_idx in rows_to_remove.iter() {
                col.data.remove(*row_idx);
            }
        }

        Ok(())
    }
}
