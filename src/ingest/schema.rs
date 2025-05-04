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

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnSchema {

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {

}