use std::collections::HashSet;

use indexmap::IndexMap;
use thiserror::Error;

use crate::types::ColumnEncoding;
use crate::types::Datatype;

pub type Schema = IndexMap<String, ColumnType>;

pub struct Table {
    columns: IndexMap<String, ColumnType>,
    pub sort_order: Option<Vec<String>>,
}

impl Table {
    pub fn has_column(&self, column: String) -> bool {
        self.columns.contains_key(&column)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnType {
    Int32,
    Int64,
    Decimal128,
    Float64,
    String,
    Binary,
    Hex,
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match *self {
            ColumnType::Int32 => "int32",
            ColumnType::Int64 => "int64",
            ColumnType::Decimal128 => "decimal128",
            ColumnType::Float64 => "float64",
            ColumnType::String => "string",
            ColumnType::Binary => "binary",
            ColumnType::Hex => "hex",
        }
    }
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Invalid column")]
    InvalidColumn,
}

impl Datatype {
    pub fn get_schema(
        &self,
        binary_column_format: &ColumnEncoding,
        include_columns: &Option<Vec<String>>,
        exclude_columns: &Option<Vec<String>>,
    ) -> Result<Schema, SchemaError> {
        let column_types = self.dataset().column_types();
        let default_columns = self.dataset().default_columns();
        let used_columns =
            compute_used_columns(default_columns, include_columns, exclude_columns, self);
        let mut schema: Schema = IndexMap::new();
        for column in used_columns {
            let mut ctype = column_types
                .get(column.as_str())
                .ok_or(SchemaError::InvalidColumn)?;
            if (*binary_column_format == ColumnEncoding::Hex) & (ctype == &ColumnType::Binary) {
                ctype = &ColumnType::Hex;
            }
            schema.insert((*column.clone()).to_string(), *ctype);
        }
        Ok(schema)
    }
}

pub fn compute_used_columns(
    default_columns: Vec<&str>,
    include_columns: &Option<Vec<String>>,
    exclude_columns: &Option<Vec<String>>,
    datatype: &Datatype,
) -> Vec<String> {
    match (include_columns, exclude_columns) {
        (Some(include), _) if ((include.len() == 1) & include.contains(&"all".to_string())) => {
            datatype
                .dataset()
                .column_types()
                .keys()
                .map(|k| k.to_string())
                .collect()
        }
        (Some(include), Some(exclude)) => {
            let include_set: HashSet<_> = include.iter().collect();
            let exclude_set: HashSet<_> = exclude.iter().collect();
            let intersection: HashSet<_> = include_set.intersection(&exclude_set).collect();
            assert!(
                intersection.is_empty(),
                "include and exclude should be non-overlapping"
            );
            include.to_vec()
        }
        (Some(include), None) => include.to_vec(),
        (None, Some(exclude)) => {
            let exclude_set: HashSet<_> = exclude.iter().collect();
            default_columns
                .into_iter()
                .filter(|s| !exclude_set.contains(&s.to_string()))
                .map(|s| s.to_string())
                .collect()
        }
        (None, None) => default_columns.iter().map(|s| s.to_string()).collect(),
    }
}
