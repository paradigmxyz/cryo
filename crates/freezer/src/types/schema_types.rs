use std::collections::HashSet;

use indexmap::IndexMap;

pub type Schema = IndexMap<String, ColumnType>;

use crate::types::ColumnEncoding;
use crate::types::Datatype;

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

impl Datatype {
    pub fn get_schema(
        // datatype: Datatype,
        &self,
        binary_column_format: &ColumnEncoding,
        include_columns: &Option<Vec<String>>,
        exclude_columns: &Option<Vec<String>>,
    ) -> Schema {
        let column_types = self.dataset().column_types();
        let default_columns = self.dataset().default_columns();
        let used_columns = compute_used_columns(default_columns, include_columns, exclude_columns);
        let mut schema: Schema = IndexMap::new();
        used_columns.iter().for_each(|column| {
            let mut column_type = *column_types.get(column.as_str()).unwrap();
            if (*binary_column_format == ColumnEncoding::Hex) & (column_type == ColumnType::Binary)
            {
                column_type = ColumnType::Hex;
            }
            schema.insert((*column.clone()).to_string(), column_type);
        });
        schema
    }
}

pub fn compute_used_columns(
    default_columns: Vec<&str>,
    include_columns: &Option<Vec<String>>,
    exclude_columns: &Option<Vec<String>>,
) -> Vec<String> {
    match (include_columns, exclude_columns) {
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
