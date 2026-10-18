/// types and functions related to schemas
use std::collections::HashMap;

use crate::{err, CollectError, ColumnEncoding, Datatype, LogDecoder};
use alloy::dyn_abi::DynSolType;
use indexmap::{IndexMap, IndexSet};
use thiserror::Error;

/// collection of schemas
pub type Schemas = HashMap<Datatype, Table>;

/// functions for Schemas
pub trait SchemaFunctions {
    /// get schema
    fn get_schema(&self, datatype: &Datatype) -> Result<&Table, CollectError>;
}

impl SchemaFunctions for HashMap<Datatype, Table> {
    fn get_schema(&self, datatype: &Datatype) -> Result<&Table, CollectError> {
        self.get(datatype).ok_or(err(format!("schema for {} missing", datatype.name()).as_str()))
    }
}

/// type related config of table
#[derive(Clone, Debug, PartialEq)]
pub struct TableConfig {
    /// representations to use for u256 columns
    pub u256_types: Vec<U256Type>,

    /// representation to use for binary columns
    pub binary_type: ColumnEncoding,

    /// whether to include hex prefix for hex columns
    pub hex_prefix: bool,
    // pub hex_upper: bool,
}

/// Schema for a particular table
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    columns: IndexMap<String, ColumnType>,

    /// datatype of Table
    pub datatype: Datatype,

    /// sort order for rows
    pub sort_columns: Option<Vec<String>>,

    /// type config for table
    pub config: TableConfig,

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
    /// Binary representation, but with a variant name,
    /// for U256, suffix would be _u256binary,
    /// for I256, suffix would be _i256binary.
    NamedBinary,
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
    pub fn to_columntype(&self, column_encoding: ColumnEncoding) -> ColumnType {
        match self {
            U256Type::Binary | U256Type::NamedBinary => match column_encoding {
                ColumnEncoding::Binary => ColumnType::Binary,
                ColumnEncoding::Hex => ColumnType::Hex,
            },
            U256Type::String => ColumnType::String,
            U256Type::F32 => ColumnType::Float32,
            U256Type::F64 => ColumnType::Float64,
            U256Type::U32 => ColumnType::UInt32,
            U256Type::U64 => ColumnType::UInt64,
            U256Type::Decimal128 => ColumnType::Decimal128,
        }
    }

    /// get column name suffix of U256Type
    pub fn suffix(&self, original_type: ColumnType) -> String {
        match self {
            U256Type::Binary => "_binary".to_string(),
            U256Type::NamedBinary => match original_type {
                ColumnType::UInt256 => "_u256binary".to_string(),
                ColumnType::Int256 => "_i256binary".to_string(),
                ColumnType::Binary => unreachable!("recursive binary type suffix"),
                _ => format!("_{}binary", original_type.as_str()),
            },
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
    /// I256 column type
    Int256,
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
    /// check if column is UInt256 or Int256
    pub fn is_256(&self) -> bool {
        matches!(self, ColumnType::UInt256 | ColumnType::Int256)
    }

    /// convert ColumnType to str
    pub fn as_str(&self) -> &'static str {
        match *self {
            ColumnType::Boolean => "bool",
            ColumnType::UInt32 => "uint32",
            ColumnType::UInt64 => "uint64",
            ColumnType::UInt256 => "uint256",
            ColumnType::Int32 => "int32",
            ColumnType::Int64 => "int64",
            ColumnType::Int256 => "int256",
            ColumnType::Float32 => "float32",
            ColumnType::Float64 => "float64",
            ColumnType::Decimal128 => "decimal128",
            ColumnType::String => "string",
            ColumnType::Binary => "binary",
            ColumnType::Hex => "hex",
        }
    }

    /// Convert [`DynSolType`] to [`ColumnType`]
    pub fn from_sol_type(
        sol_type: &DynSolType,
        binary_type: ColumnEncoding,
    ) -> Result<Self, SchemaError> {
        let result = match sol_type {
            DynSolType::Address | DynSolType::Bytes | DynSolType::FixedBytes(_) => {
                match binary_type {
                    ColumnEncoding::Binary => ColumnType::Binary,
                    ColumnEncoding::Hex => ColumnType::Hex,
                }
            }
            DynSolType::Int(bits) => {
                if *bits <= 64 {
                    ColumnType::Int64
                } else {
                    ColumnType::Int256
                }
            }
            DynSolType::Uint(bits) => {
                if *bits <= 64 {
                    ColumnType::UInt64
                } else {
                    ColumnType::UInt256
                }
            }
            DynSolType::Bool => ColumnType::Boolean,
            DynSolType::String => ColumnType::String,
            DynSolType::Array(_) => return Err(SchemaError::InvalidSolType("Array")),
            DynSolType::FixedArray(_, _) => return Err(SchemaError::InvalidSolType("FixedArray")),
            DynSolType::Tuple(_) => return Err(SchemaError::InvalidSolType("Tuple")),
            DynSolType::Function => return Err(SchemaError::InvalidSolType("Function")),
            // _ => return Err(SchemaError::InvalidSolType("Unknown")),
        };
        Ok(result)
    }
}

/// Error related to Schemas
#[derive(Error, Debug)]
pub enum SchemaError {
    /// Invalid column being operated on
    #[error("Invalid column")]
    InvalidColumn(String),
    /// Error converting column type
    #[error("Invalid column type, {0}")]
    InvalidSolType(&'static str),
    /// Conflict column names
    #[error("Conflict column names: {0:?}")]
    ConflictColumnNames(Vec<String>),
}

impl Datatype {
    /// get default table schema for a particular datatype
    pub fn default_table_schema(&self, config: TableConfig) -> Table {
        let column_types =
            self.column_types().into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        Table {
            datatype: *self,
            sort_columns: None,
            columns: column_types,
            config,
            log_decoder: None,
        }
    }

    /// get schema for a particular datatype
    #[allow(clippy::too_many_arguments)]
    pub fn table_schema(
        &self,
        config: TableConfig,
        include_columns: &Option<Vec<String>>,
        exclude_columns: &Option<Vec<String>>,
        columns: &Option<Vec<String>>,
        sort: Option<Vec<String>>,
        log_decoder: Option<LogDecoder>,
    ) -> Result<Table, SchemaError> {
        let column_types = self.column_types();
        let mut additional_column_types = IndexMap::new();
        let mut all_columns: IndexSet<_> = column_types.keys().map(|k| k.to_string()).collect();
        let mut default_columns: Vec<_> =
            self.default_columns().iter().map(|s| s.to_string()).collect();
        if let Some(log_decoder) = &log_decoder {
            let event_names = log_decoder
                .field_names()
                .into_iter()
                .map(|s| format!("event__{s}"))
                .collect::<Vec<_>>();
            default_columns.extend(event_names.clone());
            let expected_len = all_columns.len() + event_names.len();
            all_columns.extend(event_names.clone());
            if all_columns.len() != expected_len {
                return Err(SchemaError::ConflictColumnNames(event_names.clone()));
            }
            let drop_names = [
                "topic1".to_string(),
                "topic2".to_string(),
                "topic3".to_string(),
                "data".to_string(),
            ];
            default_columns.retain(|i| !drop_names.contains(i));
            additional_column_types.extend(
                log_decoder
                    .field_names_and_types()
                    .into_iter()
                    .map(|(name, ty)| (format!("event__{name}"), ty)),
            );
        }
        let used_columns = compute_used_columns(
            all_columns,
            default_columns,
            include_columns,
            exclude_columns,
            columns,
        );
        let mut columns = IndexMap::new();
        for column in used_columns {
            let mut ctype = match column_types.get(column.as_str()) {
                Some(ctype) => *ctype,
                None => {
                    let sol_type = additional_column_types
                        .get(column.as_str())
                        .ok_or_else(|| SchemaError::InvalidColumn(column.clone()))?;
                    ColumnType::from_sol_type(sol_type, config.binary_type)?
                }
            };
            if config.binary_type == ColumnEncoding::Hex && ctype == ColumnType::Binary {
                ctype = ColumnType::Hex;
            }
            columns.insert((*column.clone()).to_string(), ctype);
        }

        let schema = Table { datatype: *self, sort_columns: sort, columns, config, log_decoder };
        Ok(schema)
    }
}

fn compute_used_columns(
    all_columns: IndexSet<String>,
    default_columns: Vec<String>,
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
            result_set.clear();
            result_set.extend(all_columns.iter().cloned());
        } else {
            // Permissively skip `include` columns that are not in this dataset (they might apply to
            // other dataset)
            result_set.extend(include.iter().cloned());
        }
    }
    result_set = result_set.intersection(&all_columns).cloned().collect();
    if let Some(exclude) = exclude_columns {
        let exclude_set = IndexSet::<String>::from_iter(exclude.iter().cloned());
        result_set = result_set.difference(&exclude_set).cloned().collect()
    }
    result_set
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_config() -> TableConfig {
        TableConfig {
            u256_types: vec![U256Type::Binary, U256Type::String, U256Type::F64],
            binary_type: ColumnEncoding::Hex,
            hex_prefix: true,
        }
    }

    #[test]
    fn test_table_schema_explicit_cols() {
        let cols = Some(vec!["block_number".to_string(), "block_hash".to_string()]);
        let table =
            Datatype::Blocks.table_schema(get_config(), &None, &None, &cols, None, None).unwrap();
        assert_eq!(vec!["block_number", "block_hash"], table.columns());

        // "all" marker support
        let cols = Some(vec!["all".to_string()]);
        let table =
            Datatype::Blocks.table_schema(get_config(), &None, &None, &cols, None, None).unwrap();
        assert_eq!(21, table.columns().len());
        assert!(table.columns().contains(&"block_hash"));
        assert!(table.columns().contains(&"transactions_root"));
    }

    #[test]
    fn test_table_schema_include_cols() {
        let inc_cols = Some(vec!["chain_id".to_string(), "receipts_root".to_string()]);
        let table = Datatype::Blocks
            .table_schema(get_config(), &inc_cols, &None, &None, None, None)
            .unwrap();
        assert_eq!(9, table.columns().len());
        assert_eq!(["chain_id", "receipts_root"], table.columns()[7..9]);

        // Non-existing include is skipped
        let inc_cols = Some(vec!["chain_id".to_string(), "foo_bar".to_string()]);
        let table = Datatype::Blocks
            .table_schema(get_config(), &inc_cols, &None, &None, None, None)
            .unwrap();
        assert_eq!(Some(&"chain_id"), table.columns().last());
        assert!(!table.columns().contains(&"foo_bar"));

        // "all" marker support
        let inc_cols = Some(vec!["all".to_string()]);
        let table = Datatype::Blocks
            .table_schema(get_config(), &inc_cols, &None, &None, None, None)
            .unwrap();
        assert_eq!(21, table.columns().len());
        assert!(table.columns().contains(&"block_hash"));
        assert!(table.columns().contains(&"transactions_root"));
    }

    #[test]
    fn test_table_schema_exclude_cols() {
        // defaults
        let table =
            Datatype::Blocks.table_schema(get_config(), &None, &None, &None, None, None).unwrap();
        assert_eq!(8, table.columns().len());
        assert!(table.columns().contains(&"author"));
        assert!(table.columns().contains(&"extra_data"));

        let ex_cols = Some(vec!["author".to_string(), "extra_data".to_string()]);
        let table = Datatype::Blocks
            .table_schema(get_config(), &None, &ex_cols, &None, None, None)
            .unwrap();
        assert_eq!(6, table.columns().len());
        assert!(!table.columns().contains(&"author"));
        assert!(!table.columns().contains(&"extra_data"));

        // Non-existing exclude is ignored
        let ex_cols = Some(vec!["timestamp".to_string(), "foo_bar".to_string()]);
        let table = Datatype::Blocks
            .table_schema(get_config(), &None, &ex_cols, &None, None, None)
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
            .table_schema(get_config(), &inc_cols, &ex_cols, &None, None, None)
            .unwrap();
        assert!(!table.columns().contains(&"author"));
        assert!(!table.columns().contains(&"extra_data"));
        assert_eq!(7, table.columns().len());
        assert_eq!(["chain_id", "receipts_root"], table.columns()[5..7]);
    }

    #[test]
    fn test_table_schema_log_decoder() {
        let table = Datatype::Logs
            .table_schema(
                get_config(),
                &None,
                &None,
                &None,
                None,
                Some(
                    LogDecoder::new(
                        "Transfer(address indexed from, address indexed to, uint256 value)"
                            .to_string(),
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        assert!(!table.columns().contains(&"topic1"));
        assert!(!table.columns().contains(&"topic2"));
        assert!(!table.columns().contains(&"topic3"));
        assert!(!table.columns().contains(&"data"));
        assert_eq!(table.columns().len(), 11);
        assert_eq!(["event__from", "event__to", "event__value"], table.columns()[8..11]);
    }

    #[test]
    fn test_table_schema_log_decoder_include_exclude_cols() {
        let inc_cols = vec!["topic1".to_string(), "data".to_string()];
        let ex_cols = vec!["event__arg0".to_string()];
        let table = Datatype::Logs
            .table_schema(
                get_config(),
                &Some(inc_cols),
                &Some(ex_cols),
                &None,
                None,
                Some(
                    LogDecoder::new(
                        "Transfer(address indexed,address indexed,uint256)".to_string(),
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        assert!(table.columns().contains(&"topic1"));
        assert!(!table.columns().contains(&"topic2"));
        assert!(!table.columns().contains(&"topic3"));
        assert!(table.columns().contains(&"data"));
        assert_eq!(table.columns().len(), 12);
        assert_eq!(["event__arg1", "event__arg2"], table.columns()[8..10]);
    }
}
