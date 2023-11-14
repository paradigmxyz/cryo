use crate::{err, CollectError, ColumnEncoding, ToU256Series, U256Type};
use ethers::prelude::*;
use ethers_core::abi::{AbiEncode, EventParam, HumanReadableParser, ParamType, RawLog, Token};
use polars::prelude::*;
use std::collections::HashSet;

/// container for log decoding context
#[derive(Clone, Debug, PartialEq)]
pub struct LogDecoder {
    /// the raw event signature string ex: event Transfer(address indexed from, address indexed to,
    /// uint256 amount)
    pub raw: String,
    /// decoded abi type of event signature string
    pub event: abi::Event,
}

impl LogDecoder {
    /// create a new LogDecoder from an event signature
    /// ex: LogDecoder::new("event Transfer(address indexed from, address indexed to, uint256
    /// amount)".to_string())
    pub fn new(event_signature: String) -> Result<Self, String> {
        match HumanReadableParser::parse_event(event_signature.as_str()) {
            Ok(event) => Ok(Self { event, raw: event_signature.clone() }),
            Err(e) => {
                let err = format!("incorrectly formatted event {} (expect something like event Transfer(address indexed from, address indexed to, uint256 amount) err: {}", event_signature, e);
                eprintln!("{}", err);
                Err(err)
            }
        }
    }

    /// get field names of event inputs
    pub fn field_names(&self) -> Vec<String> {
        self.event.inputs.iter().map(|i| i.name.clone()).collect()
    }

    /// converts from a log type to an abi token type
    /// this function assumes all logs are of the same type and skips fields if they don't match the
    /// passed event definition
    pub fn parse_log_from_event(&self, logs: Vec<Log>) -> indexmap::IndexMap<String, Vec<Token>> {
        let mut map: indexmap::IndexMap<String, Vec<Token>> = indexmap::IndexMap::new();
        let known_keys =
            self.event.inputs.clone().into_iter().map(|i| i.name).collect::<HashSet<String>>();

        for log in logs {
            match self.event.parse_log(RawLog::from(log)) {
                Ok(log) => {
                    for param in log.params {
                        if known_keys.contains(param.name.as_str()) {
                            let tokens = map.entry(param.name).or_default();
                            tokens.push(param.value);
                        }
                    }
                }
                Err(e) => eprintln!("error parsing log: {:?}", e),
            }
        }
        map
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn make_series(
        &self,
        name: String,
        data: Vec<Token>,
        chunk_len: usize,
        u256_types: &[U256Type],
        column_encoding: &ColumnEncoding,
    ) -> Result<Vec<Series>, CollectError> {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut str_ints: Vec<String> = vec![];
        let mut u256s: Vec<U256> = vec![];
        let mut i256s: Vec<I256> = vec![];
        let mut bytes: Vec<Vec<u8>> = vec![];
        let mut hexes: Vec<String> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        // TODO: support array & tuple types

        let param = self
            .event
            .inputs
            .clone()
            .into_iter()
            .filter(|i| i.name == name)
            .collect::<Vec<EventParam>>();
        let param = param.first();

        for token in data {
            match token {
                Token::Address(a) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(a.to_fixed_bytes().into()),
                    ColumnEncoding::Hex => hexes.push(format!("{:?}", a)),
                },
                Token::FixedBytes(b) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b),
                    ColumnEncoding::Hex => hexes.push(b.encode_hex()),
                },
                Token::Bytes(b) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b),
                    ColumnEncoding::Hex => hexes.push(b.encode_hex()),
                },
                Token::Uint(i) => match param {
                    Some(param) => match param.kind.clone() {
                        ParamType::Uint(size) => {
                            if size <= 64 {
                                uints.push(i.as_u64())
                            } else {
                                u256s.push(i)
                            }
                        }
                        _ => str_ints.push(i.to_string()),
                    },
                    None => match i.try_into() {
                        Ok(i) => ints.push(i),
                        Err(_) => str_ints.push(i.to_string()),
                    },
                },
                Token::Int(i) => {
                    let i = I256::from_raw(i);
                    match param {
                        Some(param) => match param.kind.clone() {
                            ParamType::Int(size) => {
                                if size <= 64 {
                                    ints.push(i.as_i64())
                                } else {
                                    i256s.push(i)
                                }
                            }
                            _ => str_ints.push(i.to_string()),
                        },
                        None => match i.try_into() {
                            Ok(i) => ints.push(i),
                            Err(_) => str_ints.push(i.to_string()),
                        },
                    }
                }
                Token::Bool(b) => bools.push(b),
                Token::String(s) => strings.push(s),
                Token::Array(_) | Token::FixedArray(_) => {}
                Token::Tuple(_) => {}
            }
        }
        let mixed_length_err = format!("could not parse column {}, mixed type", name);
        let mixed_length_err = mixed_length_err.as_str();

        // check each vector, see if it contains any values, if it does, check if it's the same
        // length as the input data and map to a series
        let name = format!("event__{}", name);
        if !ints.is_empty() {
            Ok(vec![Series::new(name.as_str(), ints)])
        } else if !i256s.is_empty() {
            let mut series_vec = Vec::new();
            for u256_type in u256_types.iter() {
                series_vec.push(i256s.to_u256_series(
                    name.clone(),
                    u256_type.clone(),
                    column_encoding,
                )?)
            }
            Ok(series_vec)
        } else if !u256s.is_empty() {
            let mut series_vec: Vec<Series> = Vec::new();
            for u256_type in u256_types.iter() {
                series_vec.push(u256s.to_u256_series(
                    name.clone(),
                    u256_type.clone(),
                    column_encoding,
                )?)
            }
            Ok(series_vec)
        } else if !uints.is_empty() {
            Ok(vec![Series::new(name.as_str(), uints)])
        } else if !str_ints.is_empty() {
            Ok(vec![Series::new(name.as_str(), str_ints)])
        } else if !bytes.is_empty() {
            if bytes.len() != chunk_len {
                return Err(err(mixed_length_err))
            }
            Ok(vec![Series::new(name.as_str(), bytes)])
        } else if !hexes.is_empty() {
            if hexes.len() != chunk_len {
                return Err(err(mixed_length_err))
            }
            Ok(vec![Series::new(name.as_str(), hexes)])
        } else if !bools.is_empty() {
            if bools.len() != chunk_len {
                return Err(err(mixed_length_err))
            }
            Ok(vec![Series::new(name.as_str(), bools)])
        } else if !strings.is_empty() {
            if strings.len() != chunk_len {
                return Err(err(mixed_length_err))
            }
            Ok(vec![Series::new(name.as_str(), strings)])
        } else {
            // case where no data was passed
            Ok(vec![Series::new(name.as_str(), vec![None::<u64>; chunk_len])])
        }
    }
}
