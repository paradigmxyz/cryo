use ethers::prelude::*;
use ethers_core::abi::{AbiEncode, EventParam, HumanReadableParser, ParamType, RawLog, Token};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};

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

    // fn field_names(&self) -> Vec<String> {
    //     self.event.inputs.iter().map(|i| i.name.clone()).collect()
    // }

    /// converts from a log type to an abi token type
    /// this function assumes all logs are of the same type and skips fields if they don't match the
    /// passed event definition
    pub fn parse_log_from_event(&self, logs: Vec<Log>) -> HashMap<String, Vec<Token>> {
        let mut map: HashMap<String, Vec<Token>> = HashMap::new();
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
    ) -> Result<Series, String> {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut str_ints: Vec<String> = vec![];
        let mut bytes: Vec<String> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        let mut addresses: Vec<String> = vec![];
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
                Token::Address(a) => addresses.push(format!("{:?}", a)),
                Token::FixedBytes(b) => bytes.push(b.encode_hex()),
                Token::Bytes(b) => bytes.push(b.encode_hex()),
                Token::Uint(i) => match param {
                    Some(param) => {
                        match param.kind.clone() {
                            ParamType::Uint(size) => {
                                if size <= 64 {
                                    uints.push(i.as_u64())
                                } else {
                                    // TODO: decode this based on U256Types flag
                                    str_ints.push(i.to_string())
                                }
                            }
                            _ => str_ints.push(i.to_string()),
                        }
                    }
                    None => match i.try_into() {
                        Ok(i) => ints.push(i),
                        Err(_) => str_ints.push(i.to_string()),
                    },
                },
                Token::Int(i) => match param {
                    Some(param) => {
                        match param.kind.clone() {
                            ParamType::Int(size) => {
                                if size <= 64 {
                                    match i.as_u64().try_into() {
                                        Ok(i) => ints.push(i),
                                        Err(_) => str_ints.push(i.to_string()),
                                    }
                                } else {
                                    // TODO: decode this based on U256Types flag
                                    str_ints.push(i.to_string())
                                }
                            }
                            _ => str_ints.push(i.to_string()),
                        }
                    }
                    None => match i.try_into() {
                        Ok(i) => ints.push(i),
                        Err(_) => str_ints.push(i.to_string()),
                    },
                },
                Token::Bool(b) => bools.push(b),
                Token::String(s) => strings.push(s),
                Token::Array(_) | Token::FixedArray(_) => {}
                Token::Tuple(_) => {}
            }
        }
        let mixed_length_err = format!("could not parse column {}, mixed type", name);

        // check each vector, see if it contains any values, if it does, check if it's the same
        // length as the input data and map to a series
        if !ints.is_empty() {
            Ok(Series::new(name.as_str(), ints))
        } else if !uints.is_empty() {
            Ok(Series::new(name.as_str(), uints))
        } else if !str_ints.is_empty() {
            Ok(Series::new(name.as_str(), str_ints))
        } else if !bytes.is_empty() {
            if bytes.len() != chunk_len {
                return Err(mixed_length_err)
            }
            Ok(Series::new(name.as_str(), bytes))
        } else if !bools.is_empty() {
            if bools.len() != chunk_len {
                return Err(mixed_length_err)
            }
            Ok(Series::new(name.as_str(), bools))
        } else if !strings.is_empty() {
            if strings.len() != chunk_len {
                return Err(mixed_length_err)
            }
            Ok(Series::new(name.as_str(), strings))
        } else if !addresses.is_empty() {
            if addresses.len() != chunk_len {
                return Err(mixed_length_err)
            }
            Ok(Series::new(name.as_str(), addresses))
        } else {
            // case where no data was passed
            Ok(Series::new(name.as_str(), vec![None::<u64>; chunk_len]))
        }
    }
}
