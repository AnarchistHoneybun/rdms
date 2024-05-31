use crate::database::Database;
use crate::database::db_errors::Error;
use crate::table::helpers::evaluate_nested_conditions;
use crate::table::NestedCondition;

impl Database {
    pub fn delete_with_nested_conditions_in_table(
        &mut self,
        table_name: &str,
        nested_condition: NestedCondition,
    ) -> Result<(), Error> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(Error::TableNotFound(table_name.to_owned()))?;

        let primary_key_column_idx = table
            .columns
            .iter()
            .position(|col| col.is_primary_key)
            .ok_or(Error::NoPrimaryKeyColumn(table_name.to_owned()))?;

        let mut primary_key_values_to_delete = Vec::new();

        for row_idx in 0..table.columns[0].data.len() {
            if evaluate_nested_conditions(&nested_condition, &table.columns, row_idx)? {
                let primary_key_value = table.columns[primary_key_column_idx]
                    .data
                    .get(row_idx)
                    .cloned()
                    .ok_or(Error::MissingPrimaryKeyValue)?;
                primary_key_values_to_delete.push(primary_key_value);
            }
        }

        let table_foreign_key_data = table.referenced_as_foreign_key.clone();
        table.delete_with_nested_conditions(&nested_condition)?;

        for (ref_table_name, ref_column_name) in table_foreign_key_data {
            for primary_key_value in &primary_key_values_to_delete {
                let ref_nested_condition = NestedCondition::Condition(
                    ref_column_name.clone(),
                    "=".to_string(),
                    primary_key_value.to_string(),
                );

                self.delete_with_nested_conditions_in_table(&ref_table_name, ref_nested_condition)?;
            }
        }

        Ok(())
    }
}