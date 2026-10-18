use alloy::{
    dyn_abi::{DynSolType, DynSolValue, EventExt},
    json_abi::{Event, EventParam},
    rpc::types::Log,
};

/// container for log decoding context
#[derive(Clone, Debug, PartialEq)]
pub struct LogDecoder {
    /// the raw event signature string ex: event Transfer(address indexed from, address indexed to,
    /// uint256 amount)
    pub raw: String,
    /// decoded abi type of event signature string
    pub event: Event,
}

fn get_param_name(param: &EventParam, idx: usize) -> String {
    if param.name.is_empty() {
        format!("arg{idx}")
    } else {
        param.name.clone()
    }
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
                error!("{err}");
                Err(err)
            }
        }
    }

    /// get field names of event inputs
    pub fn field_names(&self) -> Vec<String> {
        self.event
            .inputs
            .iter()
            .enumerate()
            .map(|(idx, param)| get_param_name(param, idx))
            .collect()
    }

    /// get field names of indexed event inputs
    pub fn indexed_names(&self) -> Vec<String> {
        self.event
            .inputs
            .iter()
            .enumerate()
            .filter_map(
                |(idx, param)| if param.indexed { Some(get_param_name(param, idx)) } else { None },
            )
            .collect()
    }

    /// get field names of non-indexed event inputs
    pub fn body_names(&self) -> Vec<String> {
        self.event
            .inputs
            .iter()
            .enumerate()
            .filter_map(
                |(idx, param)| if !param.indexed { Some(get_param_name(param, idx)) } else { None },
            )
            .collect()
    }

    /// get field types of event inputs
    pub fn field_types(&self) -> Vec<DynSolType> {
        self.event.inputs.iter().map(|i| DynSolType::parse(&i.ty).unwrap()).collect()
    }

    /// get field names and types of event inputs
    pub fn field_names_and_types(&self) -> Vec<(String, DynSolType)> {
        self.event
            .inputs
            .iter()
            .enumerate()
            .map(|(idx, param)| (get_param_name(param, idx), DynSolType::parse(&param.ty).unwrap()))
            .collect()
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
                Err(e) => error!("error parsing log: {e:?}"),
            }
        }
        map
    }
}
