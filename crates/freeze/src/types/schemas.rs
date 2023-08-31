/// types and functions related to schemas
use std::collections::HashSet;

use indexmap::IndexMap;
use thiserror::Error;

use crate::types::{ColumnEncoding, Datatype};

/// Schema for a particular table
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    columns: IndexMap<String, ColumnType>,

    /// datatype of Table
    pub datatype: Datatype,

    /// sort order for rows
    pub sort_columns: Option<Vec<String>>,

    /// representations to use for u256 columns
    pub u256_types: HashSet<U256Type>,

    /// representation to use for binary columns
    pub binary_type: ColumnEncoding,
}

impl Table {
    /// return whether schema has a column
    pub fn has_column(&self, column: &str) -> bool {
        self.columns.contains_key(column)
    }

    /// get ColumnType of column
    pub fn column_type(&self, column: &str) -> Option<ColumnType> {
        self.columns.get(column).cloned()
    }

    /// get columns of Table
    pub fn columns(&self) -> Vec<&str> {
        self.columns.keys().map(|x| x.as_str()).collect()
    }
}

/// representation of a U256 datum
#[derive(Hash, Clone, Debug, Eq, PartialEq)]
pub enum U256Type {
    /// Binary representation
    Binary,
    /// String representation
    String,
    /// F32 representation
    F32,
    /// F64 representation
    F64,
    /// U32 representation
    U32,
    /// U64 representation
    U64,
    /// Decimal128 representation
    Decimal128,
}

impl U256Type {
    /// convert U256Type to Columntype
    pub fn to_columntype(&self) -> ColumnType {
        match self {
            U256Type::Binary => ColumnType::Binary,
            U256Type::String => ColumnType::String,
            U256Type::F32 => ColumnType::Float32,
            U256Type::F64 => ColumnType::Float64,
            U256Type::U32 => ColumnType::UInt32,
            U256Type::U64 => ColumnType::UInt64,
            U256Type::Decimal128 => ColumnType::Decimal128,
        }
    }

    /// get column name suffix of U256Type
    pub fn suffix(&self) -> String {
        match self {
            U256Type::Binary => "_binary".to_string(),
            U256Type::String => "_string".to_string(),
            U256Type::F32 => "_f32".to_string(),
            U256Type::F64 => "_f64".to_string(),
            U256Type::U32 => "_u32".to_string(),
            U256Type::U64 => "_u64".to_string(),
            U256Type::Decimal128 => "_d128".to_string(),
        }
    }
}

/// datatype of column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    /// UInt32 column type
    UInt32,
    /// UInt64 column type
    UInt64,
    /// U256 column type
    UInt256,
    /// Int32 column type
    Int32,
    /// Int64 column type
    Int64,
    /// Float32 column type
    Float32,
    /// Float64 column type
    Float64,
    /// Decimal128 column type
    Decimal128,
    /// String column type
    String,
    /// Binary column type
    Binary,
    /// Hex column type
    Hex,
}

impl ColumnType {
    /// convert ColumnType to str
    pub fn as_str(&self) -> &'static str {
        match *self {
            ColumnType::UInt32 => "uint32",
            ColumnType::UInt64 => "uint64",
            ColumnType::UInt256 => "uint256",
            ColumnType::Int32 => "int32",
            ColumnType::Int64 => "int64",
            ColumnType::Float32 => "float32",
            ColumnType::Float64 => "float64",
            ColumnType::Decimal128 => "decimal128",
            ColumnType::String => "string",
            ColumnType::Binary => "binary",
            ColumnType::Hex => "hex",
        }
    }
}

/// Error related to Schemas
#[derive(Error, Debug)]
pub enum SchemaError {
    /// Invalid column being operated on
    #[error("Invalid column")]
    InvalidColumn,
}

impl Datatype {
    /// get schema for a particular datatype
    pub fn table_schema(
        &self,
        u256_types: &HashSet<U256Type>,
        binary_column_format: &ColumnEncoding,
        include_columns: &Option<Vec<String>>,
        exclude_columns: &Option<Vec<String>>,
        columns: &Option<Vec<String>>,
        sort: Option<Vec<String>>,
    ) -> Result<Table, SchemaError> {
        let column_types = self.dataset().column_types();
        let default_columns = self.dataset().default_columns();
        let used_columns =
            compute_used_columns(default_columns, include_columns, exclude_columns, columns, self);
        let mut columns = IndexMap::new();
        for column in used_columns {
            let mut ctype = column_types.get(column.as_str()).ok_or(SchemaError::InvalidColumn)?;
            if (*binary_column_format == ColumnEncoding::Hex) & (ctype == &ColumnType::Binary) {
                ctype = &ColumnType::Hex;
            }
            columns.insert((*column.clone()).to_string(), *ctype);
        }
        let schema = Table {
            datatype: *self,
            sort_columns: sort,
            columns,
            u256_types: u256_types.clone(),
            binary_type: binary_column_format.clone(),
        };
        Ok(schema)
    }
}

fn compute_used_columns(
    default_columns: Vec<&str>,
    include_columns: &Option<Vec<String>>,
    exclude_columns: &Option<Vec<String>>,
    columns: &Option<Vec<String>>,
    datatype: &Datatype,
) -> Vec<String> {
    match (columns, include_columns, exclude_columns) {
        (Some(columns), _, _) if ((columns.len() == 1) & columns.contains(&"all".to_string())) => {
            datatype.dataset().column_types().keys().map(|k| k.to_string()).collect()
        }
        (Some(columns), _, _) => columns.iter().map(|x| x.to_string()).collect(),
        (_, Some(include), _) if ((include.len() == 1) & include.contains(&"all".to_string())) => {
            datatype.dataset().column_types().keys().map(|k| k.to_string()).collect()
        }
        (_, Some(include), Some(exclude)) => {
            let mut result: Vec<String> = default_columns.iter().map(|s| s.to_string()).collect();
            let mut result_set: HashSet<String> = result.iter().cloned().collect();
            let exclude_set: HashSet<String> = exclude.iter().cloned().collect();
            include
                .iter()
                .filter(|item| !exclude_set.contains(*item) && result_set.insert(item.to_string()))
                .for_each(|item| result.push(item.clone()));
            result
        }
        (_, Some(include), None) => {
            let mut result: Vec<String> = default_columns.iter().map(|s| s.to_string()).collect();
            let mut result_set: HashSet<String> = result.iter().cloned().collect();
            include
                .iter()
                .filter(|item| result_set.insert(item.to_string()))
                .for_each(|item| result.push(item.clone()));
            result
        }
        (_, None, Some(exclude)) => {
            let exclude_set: HashSet<_> = exclude.iter().collect();
            default_columns
                .into_iter()
                .filter(|s| !exclude_set.contains(&s.to_string()))
                .map(|s| s.to_string())
                .collect()
        }
        (_, None, None) => default_columns.iter().map(|s| s.to_string()).collect(),
    }
}
