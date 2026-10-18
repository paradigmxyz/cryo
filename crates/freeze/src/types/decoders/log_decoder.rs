use crate::{err, CollectError, ColumnEncoding, ToU256Series, U256Type};
use alloy::{
    dyn_abi::{DynSolValue, EventExt},
    hex::ToHexExt,
    json_abi::Event,
    primitives::{I256, U256},
    rpc::types::Log,
};
use polars::prelude::*;

/// container for log decoding context
#[derive(Clone, Debug, PartialEq)]
pub struct LogDecoder {
    /// the raw event signature string ex: event Transfer(address indexed from, address indexed to,
    /// uint256 amount)
    pub raw: String,
    /// decoded abi type of event signature string
    pub event: Event,
}

impl LogDecoder {
    /// create a new LogDecoder from an event signature
    /// ex: LogDecoder::new("event Transfer(address indexed from, address indexed to, uint256
    /// amount)".to_string())
    pub fn new(event_signature: String) -> Result<Self, String> {
        match Event::parse(&event_signature) {
            Ok(event) => Ok(Self { event, raw: event_signature.clone() }),
            Err(e) => {
                let err = format!("incorrectly formatted event {event_signature} (expect something like event Transfer(address indexed from, address indexed to, uint256 amount) err: {e}");
                eprintln!("{err}");
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
    pub fn parse_log_from_event(
        &self,
        logs: Vec<Log>,
    ) -> indexmap::IndexMap<String, Vec<DynSolValue>> {
        let mut map: indexmap::IndexMap<String, Vec<DynSolValue>> = indexmap::IndexMap::new();
        let indexed_keys: Vec<String> = self
            .event
            .inputs
            .clone()
            .into_iter()
            .filter_map(|x| if x.indexed { Some(x.name) } else { None })
            .collect();
        let body_keys: Vec<String> = self
            .event
            .inputs
            .clone()
            .into_iter()
            .filter_map(|x| if x.indexed { None } else { Some(x.name) })
            .collect();

        for log in logs {
            match self.event.decode_log_parts(log.topics().to_vec(), log.data().data.as_ref()) {
                Ok(decoded) => {
                    for (idx, param) in decoded.indexed.into_iter().enumerate() {
                        map.entry(indexed_keys[idx].clone()).or_default().push(param);
                    }
                    for (idx, param) in decoded.body.into_iter().enumerate() {
                        map.entry(body_keys[idx].clone()).or_default().push(param);
                    }
                }
                Err(e) => eprintln!("error parsing log: {e:?}"),
            }
        }
        map
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn make_series(
        &self,
        name: String,
        data: Vec<DynSolValue>,
        chunk_len: usize,
        u256_types: &[U256Type],
        column_encoding: &ColumnEncoding,
    ) -> Result<Vec<Series>, CollectError> {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut u256s: Vec<U256> = vec![];
        let mut i256s: Vec<I256> = vec![];
        let mut bytes: Vec<Vec<u8>> = vec![];
        let mut hexes: Vec<String> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        // TODO: support array & tuple types

        for token in data {
            match token {
                DynSolValue::Address(a) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(a.to_vec()),
                    ColumnEncoding::Hex => hexes.push(format!("{a:?}")),
                },
                DynSolValue::FixedBytes(b, _) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b.to_vec()),
                    ColumnEncoding::Hex => hexes.push(b.encode_hex()),
                },
                DynSolValue::Bytes(b) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b),
                    ColumnEncoding::Hex => hexes.push(b.encode_hex()),
                },
                DynSolValue::Uint(i, size) => {
                    if size <= 64 {
                        uints.push(i.wrapping_to::<u64>())
                    } else {
                        u256s.push(i)
                    }
                }
                DynSolValue::Int(i, size) => {
                    if size <= 64 {
                        ints.push(i.unchecked_into());
                    } else {
                        i256s.push(i);
                    }
                }
                DynSolValue::Bool(b) => bools.push(b),
                DynSolValue::String(s) => strings.push(s),
                DynSolValue::Array(_) | DynSolValue::FixedArray(_) => {}
                DynSolValue::Tuple(_) => {}
                DynSolValue::Function(_) => {}
            }
        }
        let mixed_length_err = format!("could not parse column {name}, mixed type");
        let mixed_length_err = mixed_length_err.as_str();

        // check each vector, see if it contains any values, if it does, check if it's the same
        // length as the input data and map to a series
        let name = format!("event__{name}");
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
