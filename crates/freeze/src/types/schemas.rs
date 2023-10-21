/// types and functions related to schemas
use std::collections::{HashMap, HashSet};

use crate::{err, CollectError, ColumnEncoding, Datatype, LogDecoder};
use indexmap::{IndexMap, IndexSet};
use thiserror::Error;

/// collection of schemas
pub type Schemas = HashMap<Datatype, Table>;

/// funcitons for Schemas
pub trait SchemaFunctions {
    /// get schema
    fn get_schema(&self, datatype: &Datatype) -> Result<&Table, CollectError>;
}

impl SchemaFunctions for HashMap<Datatype, Table> {
    fn get_schema(&self, datatype: &Datatype) -> Result<&Table, CollectError> {
        self.get(datatype).ok_or(err(format!("schema for {} missing", datatype.name()).as_str()))
    }
}

/// Schema for a particular table
#[derive(Clone, Debug, PartialEq)]
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

    /// log decoder for table
    pub log_decoder: Option<LogDecoder>,
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
    /// Boolean column type
    Boolean,
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
            ColumnType::Boolean => "bool",
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
    #[allow(clippy::too_many_arguments)]
    pub fn table_schema(
        &self,
        u256_types: &HashSet<U256Type>,
        binary_column_format: &ColumnEncoding,
        include_columns: &Option<Vec<String>>,
        exclude_columns: &Option<Vec<String>>,
        columns: &Option<Vec<String>>,
        sort: Option<Vec<String>>,
        log_decoder: Option<LogDecoder>,
    ) -> Result<Table, SchemaError> {
        let column_types = self.column_types();
        let all_columns = column_types.keys().map(|k| k.to_string()).collect();
        let default_columns = self.default_columns();
        let used_columns = compute_used_columns(
            all_columns,
            default_columns,
            include_columns,
            exclude_columns,
            columns,
        );
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
            log_decoder,
        };
        Ok(schema)
    }
}

fn compute_used_columns(
    all_columns: IndexSet<String>,
    default_columns: Vec<&str>,
    include_columns: &Option<Vec<String>>,
    exclude_columns: &Option<Vec<String>>,
    columns: &Option<Vec<String>>,
) -> IndexSet<String> {
    if let Some(columns) = columns {
        if (columns.len() == 1) & columns.contains(&"all".to_string()) {
            return all_columns
        }
        return columns.iter().map(|x| x.to_string()).collect()
    }
    let mut result_set = IndexSet::from_iter(default_columns.iter().map(|s| s.to_string()));
    if let Some(include) = include_columns {
        if (include.len() == 1) & include.contains(&"all".to_string()) {
            return all_columns
        }
        // Permissively skip `include` columns that are not in this dataset (they might apply to
        // other dataset)
        result_set.extend(include.iter().cloned());
        result_set = result_set.intersection(&all_columns).cloned().collect()
    }
    if let Some(exclude) = exclude_columns {
        let exclude_set = IndexSet::<String>::from_iter(exclude.iter().cloned());
        result_set = result_set.difference(&exclude_set).cloned().collect()
    }
    result_set
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_u256_types() -> HashSet<U256Type> {
        HashSet::from_iter(vec![U256Type::Binary, U256Type::String, U256Type::F64])
    }

    #[test]
    fn test_table_schema_explicit_cols() {
        let cols = Some(vec!["block_number".to_string(), "block_hash".to_string()]);
        let table = Datatype::Blocks
            .table_schema(&get_u256_types(), &ColumnEncoding::Hex, &None, &None, &cols, None, None)
            .unwrap();
        assert_eq!(vec!["block_number", "block_hash"], table.columns());

        // "all" marker support
        let cols = Some(vec!["all".to_string()]);
        let table = Datatype::Blocks
            .table_schema(&get_u256_types(), &ColumnEncoding::Hex, &None, &None, &cols, None, None)
            .unwrap();
        assert_eq!(15, table.columns().len());
        assert!(table.columns().contains(&"block_hash"));
        assert!(table.columns().contains(&"transactions_root"));
    }

    #[test]
    fn test_table_schema_include_cols() {
        let inc_cols = Some(vec!["chain_id".to_string(), "receipts_root".to_string()]);
        let table = Datatype::Blocks
            .table_schema(
                &get_u256_types(),
                &ColumnEncoding::Hex,
                &inc_cols,
                &None,
                &None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(9, table.columns().len());
        assert_eq!(["chain_id", "receipts_root"], table.columns()[7..9]);

        // Non-existing include is skipped
        let inc_cols = Some(vec!["chain_id".to_string(), "foo_bar".to_string()]);
        let table = Datatype::Blocks
            .table_schema(
                &get_u256_types(),
                &ColumnEncoding::Hex,
                &inc_cols,
                &None,
                &None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(Some(&"chain_id"), table.columns().last());
        assert!(!table.columns().contains(&"foo_bar"));

        // "all" marker support
        let inc_cols = Some(vec!["all".to_string()]);
        let table = Datatype::Blocks
            .table_schema(
                &get_u256_types(),
                &ColumnEncoding::Hex,
                &inc_cols,
                &None,
                &None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(15, table.columns().len());
        assert!(table.columns().contains(&"block_hash"));
        assert!(table.columns().contains(&"transactions_root"));
    }

    #[test]
    fn test_table_schema_exclude_cols() {
        // defaults
        let table = Datatype::Blocks
            .table_schema(&get_u256_types(), &ColumnEncoding::Hex, &None, &None, &None, None, None)
            .unwrap();
        assert_eq!(8, table.columns().len());
        assert!(table.columns().contains(&"author"));
        assert!(table.columns().contains(&"extra_data"));

        let ex_cols = Some(vec!["author".to_string(), "extra_data".to_string()]);
        let table = Datatype::Blocks
            .table_schema(
                &get_u256_types(),
                &ColumnEncoding::Hex,
                &None,
                &ex_cols,
                &None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(6, table.columns().len());
        assert!(!table.columns().contains(&"author"));
        assert!(!table.columns().contains(&"extra_data"));

        // Non-existing exclude is ignored
        let ex_cols = Some(vec!["timestamp".to_string(), "foo_bar".to_string()]);
        let table = Datatype::Blocks
            .table_schema(
                &get_u256_types(),
                &ColumnEncoding::Hex,
                &None,
                &ex_cols,
                &None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(7, table.columns().len());
        assert!(!table.columns().contains(&"timestamp"));
        assert!(!table.columns().contains(&"foo_bar"));
    }

    #[test]
    fn test_table_schema_include_and_exclude_cols() {
        let inc_cols = Some(vec!["chain_id".to_string(), "receipts_root".to_string()]);
        let ex_cols = Some(vec!["author".to_string(), "extra_data".to_string()]);
        let table = Datatype::Blocks
            .table_schema(
                &get_u256_types(),
                &ColumnEncoding::Hex,
                &inc_cols,
                &ex_cols,
                &None,
                None,
                None,
            )
            .unwrap();
        assert!(!table.columns().contains(&"author"));
        assert!(!table.columns().contains(&"extra_data"));
        assert_eq!(7, table.columns().len());
        assert_eq!(["chain_id", "receipts_root"], table.columns()[5..7]);
    }
}
