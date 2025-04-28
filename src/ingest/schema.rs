use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    BigInt,
    Double,
    String,
    Boolean,
    Date,
    Timestamp,
    Unknown(String),
}

impl DataType {
    pub fn to_sql_type(&self) -> String {
        match self {
            DataType::Integer => "INTEGER".to_string(),
            DataType::BigInt => "BIGINT".to_string(),
            DataType::Double => "DOUBLE".to_string(),
            DataType::String => "VARCHAR".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::Unknown(t) => t.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnSchema {
    pub fn to_sql_definition(&self) -> String {
        let nullable_str = if self.nullable { "" } else { " NOT NULL" };
        format!("{} {}{}", self.name, self.data_type.to_sql_type(), nullable_str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn to_create_table_sql(&self) -> String {
        let columns_sql: Vec<String> = self.columns
            .iter()
            .map(|col| col.to_sql_definition())
            .collect();

        format!(
            "CREATE TABLE {} (\n    {}\n);",
            self.name,
            columns_sql.join(",\n    ")
        )
    }

    // Generate a SQL DDL string that can be used for LLM context
    pub fn to_ddl(&self) -> String {
        self.to_create_table_sql()
    }

    // Generate SQL to infer foreign key relationships
    pub fn infer_foreign_keys_sql(&self, other_tables: &[TableSchema]) -> Vec<String> {
        let mut fk_sql = Vec::new();

        // For each column that ends with _id or is named id
        for col in &self.columns {
            if col.name.ends_with("_id") || col.name == "id" {
                // Try to find a matching table
                let potential_table = if col.name == "id" {
                    self.name.clone()
                } else {
                    col.name.trim_end_matches("_id").to_string()
                };

                // Check if the table exists in our schema list
                for other_table in other_tables {
                    if other_table.name.to_lowercase() == potential_table.to_lowercase() {
                        // Check if the other table has an id column
                        if other_table.columns.iter().any(|c| c.name == "id") {
                            fk_sql.push(format!(
                                "ALTER TABLE {} ADD CONSTRAINT fk_{}_{}_{} FOREIGN KEY ({}) REFERENCES {} (id);",
                                self.name, self.name, col.name, other_table.name, col.name, other_table.name
                            ));
                        }
                    }
                }
            }
        }

        fk_sql
    }
}