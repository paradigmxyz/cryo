use alloy::primitives::{I256, U256};
use cryo_freeze::*;
use cryo_to_df::FromDataFrames;
use polars::prelude::*;
use std::collections::HashMap;

#[derive(Default, FromDataFrames, Debug)]
pub struct Blocks {
    pub n_rows: u64,
    pub field1: Vec<u32>,
    pub field2: Vec<String>,
    pub chain_id: Vec<u64>,
}

#[derive(Default, FromDataFrames, Debug)]
pub struct Balances {
    pub n_rows: u64,
    pub value_u256: Vec<U256>,
    pub value_i256: Vec<I256>,
    pub optional_u256: Vec<Option<U256>>,
    pub optional_i256: Vec<Option<I256>>,
    pub chain_id: Vec<u64>,
}

#[derive(Default, FromDataFrames, Debug)]
pub struct BalanceReads {
    pub n_rows: u64,
    pub fallback_u256: Vec<U256>,
    pub fallback_i256: Vec<I256>,
    pub chain_id: Vec<u64>,
}

#[test]
fn test_from_dataframes_derive() {
    test_from_dataframes().unwrap();
}

#[test]
fn test_u256_i256_binary_columns() {
    test_u256_i256_from_dataframes().unwrap();
}

#[test]
fn test_binary_column_fallback() {
    test_binary_fallback().unwrap();
}

#[test]
fn test_option_vec_error_handling() {
    test_option_vec_errors().unwrap();
}

#[test]
fn test_from_binary_vec_trait() {
    test_from_binary_vec().unwrap();
}

fn get_config() -> TableConfig {
    TableConfig {
        u256_types: vec![U256Type::Binary, U256Type::String, U256Type::F64],
        binary_type: ColumnEncoding::Hex,
        hex_prefix: true,
    }
}

#[allow(dead_code)]
pub fn test_from_dataframes() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FromDataFrames derive macro...");

    // Create test data
    let field1_data = vec![1u32, 2, 3];
    let field2_data = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let chain_id_data = vec![1u64, 1, 1];

    // Create DataFrame
    let df = DataFrame::new(vec![
        Column::new("field1".into(), field1_data.clone()),
        Column::new("field2".into(), field2_data.clone()),
        Column::new("chain_id".into(), chain_id_data.clone()),
    ])?;

    // Create input for parse_dfs (using an existing datatype)
    let mut dfs = HashMap::new();
    let mut schemas = HashMap::new();
    dfs.insert(Datatype::Blocks, df);
    schemas.insert(Datatype::Blocks, Datatype::Blocks.default_table_schema(get_config()));

    let mut result = Blocks::default();
    // Call parse_dfs
    result.parse_dfs(dfs, &schemas)?;

    println!("Result n_rows: {}", result.n_rows);
    println!("Result field1: {:?}", result.field1);
    println!("Result field2: {:?}", result.field2);
    println!("Result chain_id: {:?}", result.chain_id);

    assert_eq!(result.n_rows, 3);
    assert_eq!(result.field1, field1_data);
    assert_eq!(result.field2, field2_data);
    assert_eq!(result.chain_id, chain_id_data);

    println!("FromDataFrames derive macro works correctly!");
    Ok(())
}

#[allow(dead_code)]
pub fn test_u256_i256_from_dataframes() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing U256/I256 FromDataFrames with binary columns...");

    // Create test U256/I256 values
    let u256_val = U256::from(12345u64);
    let i256_val = I256::from_raw(U256::from(67890u64));
    let i256_negative = I256::try_from(-12345i64).unwrap();

    // Convert to binary data (32 bytes each)
    let u256_binary = u256_val.to_be_bytes_vec();
    let i256_binary = i256_val.to_be_bytes::<32>().to_vec();
    let i256_negative_binary = i256_negative.to_be_bytes::<32>().to_vec();

    println!("U256 binary length: {}", u256_binary.len());
    println!("I256 binary length: {}", i256_binary.len());
    println!("I256 negative binary length: {}", i256_negative_binary.len());

    let chain_id_data = vec![1u64, 1];

    // Create DataFrame with binary columns using the naming convention
    let df = DataFrame::new(vec![
        Column::new("value_u256_u256binary".into(), vec![u256_binary.clone(), u256_binary.clone()]),
        Column::new("value_i256_i256binary".into(), vec![i256_binary.clone(), i256_binary.clone()]),
        Column::new("optional_u256_u256binary".into(), vec![Some(u256_binary.clone()), None]),
        Column::new("optional_i256_i256binary".into(), vec![Some(i256_binary.clone()), None]),
        Column::new("chain_id".into(), chain_id_data.clone()),
    ])?;

    // Create input for parse_dfs
    let mut dfs = HashMap::new();
    let mut schemas = HashMap::new();
    dfs.insert(Datatype::Balances, df);
    schemas.insert(Datatype::Balances, Datatype::Balances.default_table_schema(get_config()));

    // Call parse_dfs
    let mut result = Balances::default();
    result.parse_dfs(dfs, &schemas)?;

    println!("Result n_rows: {}", result.n_rows);
    println!("Result value_u256: {:?}", result.value_u256);
    println!("Result value_i256: {:?}", result.value_i256);
    println!("Result optional_u256: {:?}", result.optional_u256);
    println!("Result optional_i256: {:?}", result.optional_i256);

    assert_eq!(result.n_rows, 2);
    assert_eq!(result.value_u256, vec![u256_val, u256_val]);
    assert_eq!(result.value_i256, vec![i256_val, i256_val]);
    assert_eq!(result.optional_u256, vec![Some(u256_val), None]);
    assert_eq!(result.optional_i256, vec![Some(i256_val), None]);
    assert_eq!(result.chain_id, chain_id_data);

    println!("U256/I256 FromDataFrames works correctly!");
    Ok(())
}

#[allow(dead_code)]
pub fn test_negative_i256_from_dataframes() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing negative I256 FromDataFrames with binary columns...");

    // Create test I256 values including negative
    let i256_positive = I256::try_from(12345i64).unwrap();
    let i256_negative = I256::try_from(-67890i64).unwrap();

    // Convert to binary data (32 bytes each)
    let i256_positive_binary = i256_positive.to_be_bytes::<32>().to_vec();
    let i256_negative_binary = i256_negative.to_be_bytes::<32>().to_vec();

    println!("I256 positive binary length: {}", i256_positive_binary.len());
    println!("I256 negative binary length: {}", i256_negative_binary.len());

    let chain_id_data = vec![1u64, 2];

    // Create DataFrame with binary columns
    let df = DataFrame::new(vec![
        Column::new(
            "value_i256_i256binary".into(),
            vec![i256_positive_binary.clone(), i256_negative_binary.clone()],
        ),
        Column::new("chain_id".into(), chain_id_data.clone()),
    ])?;

    // Create input for parse_dfs
    let mut dfs = HashMap::new();
    let mut schemas = HashMap::new();
    dfs.insert(Datatype::BalanceDiffs, df);
    schemas
        .insert(Datatype::BalanceDiffs, Datatype::BalanceDiffs.default_table_schema(get_config()));

    // Test the conversion
    let mut result = BalanceDiffs::default();
    result.parse_dfs(dfs, &schemas)?;

    // Verify the values
    assert_eq!(result.value_i256.len(), 2);
    assert_eq!(result.value_i256[0], i256_positive);
    assert_eq!(result.value_i256[1], i256_negative);
    assert_eq!(result.chain_id, chain_id_data);

    println!("Negative I256 FromDataFrames works correctly!");
    Ok(())
}

#[derive(Default, FromDataFrames)]
struct BalanceDiffs {
    pub n_rows: u64,
    value_i256: Vec<I256>,
    chain_id: Vec<u64>,
}

#[allow(dead_code)]
pub fn test_binary_fallback() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing binary column name fallback behavior...");

    // Create test U256/I256 values
    let u256_val = U256::from(99999u64);
    let i256_val = I256::from_raw(U256::from(88888u64));

    // Convert to binary data (32 bytes each)
    let u256_binary = u256_val.to_be_bytes_vec();
    let i256_binary = i256_val.to_be_bytes::<32>().to_vec();

    println!("Testing fallback to _binary columns...");

    let chain_id_data = vec![1u64];

    // Create DataFrame with ONLY _binary columns (no _u256binary or _i256binary)
    // This should test the fallback behavior
    let df = DataFrame::new(vec![
        Column::new("fallback_u256_binary".into(), vec![u256_binary.clone()]),
        Column::new("fallback_i256_binary".into(), vec![i256_binary.clone()]),
        Column::new("chain_id".into(), chain_id_data.clone()),
    ])?;

    // Create input for parse_dfs
    let mut dfs = HashMap::new();
    let mut schemas = HashMap::new();
    dfs.insert(Datatype::BalanceReads, df);
    schemas
        .insert(Datatype::BalanceReads, Datatype::BalanceReads.default_table_schema(get_config()));

    // Call parse_dfs - this should successfully use the _binary fallback columns
    let mut result = BalanceReads::default();
    result.parse_dfs(dfs, &schemas)?;

    println!("Result n_rows: {}", result.n_rows);
    println!("Result fallback_u256: {:?}", result.fallback_u256);
    println!("Result fallback_i256: {:?}", result.fallback_i256);

    assert_eq!(result.n_rows, 1);
    assert_eq!(result.fallback_u256, vec![u256_val]);
    assert_eq!(result.fallback_i256, vec![i256_val]);
    assert_eq!(result.chain_id, chain_id_data);

    println!("Binary column fallback works correctly!");
    Ok(())
}

#[allow(dead_code)]
pub fn test_option_vec_errors() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing OptionVec error handling...");

    // Test that OptionVec with None values fails when converting to Vec<T>
    use cryo_freeze::OptionVec;

    // Create an OptionVec with Some values - should succeed
    let option_vec_some = OptionVec::Some(vec![1u32, 2, 3]);
    let result: Result<Vec<u32>, _> = option_vec_some.try_into();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), vec![1u32, 2, 3]);

    // Create an OptionVec with None values - should fail when converting to Vec<T>
    let option_vec_with_none = OptionVec::Option(vec![Some(1u32), None, Some(3)]);
    let result: Result<Vec<u32>, _> = option_vec_with_none.try_into();
    assert!(result.is_err());

    // But should succeed when converting to Vec<Option<T>>
    let option_vec_with_none = OptionVec::Option(vec![Some(1u32), None, Some(3)]);
    let result: Result<Vec<Option<u32>>, _> = option_vec_with_none.try_into();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), vec![Some(1u32), None, Some(3)]);

    println!("OptionVec error handling works correctly!");
    Ok(())
}

#[allow(dead_code)]
pub fn test_from_binary_vec() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FromBinaryVec trait...");

    use cryo_freeze::FromBinaryVec;

    // Create test U256 binary data
    let u256_val = U256::from(12345u64);
    let u256_binary = u256_val.to_be_bytes_vec();

    // Test Vec<U256> - should succeed when all values are present
    let data_complete = vec![Some(u256_binary.clone()), Some(u256_binary.clone())];
    let result: Result<Vec<U256>, _> = Vec::from_binary_vec(data_complete);
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], u256_val);
    assert_eq!(values[1], u256_val);

    // Test Vec<U256> - should fail when there's a None value
    let data_with_none = vec![Some(u256_binary.clone()), None];
    let result: Result<Vec<U256>, _> = Vec::from_binary_vec(data_with_none);
    assert!(result.is_err());

    // Test Vec<Option<U256>> - should succeed even with None values
    let data_with_none = vec![Some(u256_binary.clone()), None, Some(u256_binary.clone())];
    let result: Result<Vec<Option<U256>>, _> = Vec::from_binary_vec(data_with_none);
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0], Some(u256_val));
    assert_eq!(values[1], None);
    assert_eq!(values[2], Some(u256_val));

    // Test I256 as well
    let i256_val = I256::from_raw(U256::from(67890u64));
    let i256_binary = i256_val.to_be_bytes::<32>().to_vec();

    let data_i256 = vec![Some(i256_binary.clone())];
    let result: Result<Vec<I256>, _> = Vec::from_binary_vec(data_i256);
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values[0], i256_val);

    println!("FromBinaryVec trait works correctly!");
    Ok(())
}
