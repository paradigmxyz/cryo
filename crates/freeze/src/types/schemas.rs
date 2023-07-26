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

/// datatype of column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    /// UInt32 column type
    UInt32,
    /// UInt64 column type
    UInt64,
    /// Int32 column type
    Int32,
    /// Int64 column type
    Int64,
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
            ColumnType::Int32 => "int32",
            ColumnType::Int64 => "int64",
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
        let schema = Table { datatype: *self, sort_columns: sort, columns };
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
            let include_set: HashSet<_> = include.iter().collect();
            let exclude_set: HashSet<_> = exclude.iter().collect();
            let intersection: HashSet<_> = include_set.intersection(&exclude_set).collect();
            assert!(intersection.is_empty(), "include and exclude should be non-overlapping");
            include.to_vec()
        }
        (_, Some(include), None) => include.to_vec(),
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
