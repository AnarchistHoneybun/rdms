use crate::column::{Column, ColumnDataType, ForeignKeyInfo};


mod column;
mod table;

mod database;
#[cfg(test)]
mod tests;

fn main() {
    let mut db = database::Database::new("my_db".to_string());
    
    let columns = vec![
        Column::new("id", ColumnDataType::Integer, None, true, None),
        Column::new("name", ColumnDataType::Text, None, false, None),
        Column::new("age", ColumnDataType::Integer, None, false, None),
    ];

    db.create_table("users", columns).unwrap();

    let columns = vec![
        Column::new("user_id", ColumnDataType::Integer, None, false, ForeignKeyInfo::new("users", "id").into()),
        Column::new("address", ColumnDataType::Text, None, false, None),
    ];

    db.create_table("addresses", columns).unwrap();
}
