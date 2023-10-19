use super::{parse_schemas, partitions};
use crate::args::Args;
use cryo_freeze::{Dim, Fetcher, ParseError, Query, QueryLabels, Schemas};
use ethers::prelude::*;
use std::sync::Arc;

pub(crate) async fn parse_query<P: JsonRpcClient>(
    args: &Args,
    fetcher: Arc<Fetcher<P>>,
) -> Result<Query, ParseError> {
    let schemas = parse_schemas(args)?;

    let arg_aliases = find_arg_aliases(args, &schemas);
    let new_args =
        if !arg_aliases.is_empty() { Some(apply_arg_aliases(args, arg_aliases)?) } else { None };
    let args = new_args.as_ref().unwrap_or(args);

    let (partitions, partitioned_by, time_dimension) =
        partitions::parse_partitions(args, fetcher, &schemas).await?;
    let datatypes = cryo_freeze::cluster_datatypes(schemas.keys().cloned().collect());
    let labels = QueryLabels { align: args.align, reorg_buffer: args.reorg_buffer };
    Ok(Query { datatypes, schemas, time_dimension, partitions, partitioned_by, labels })
}

fn find_arg_aliases(args: &Args, schemas: &Schemas) -> Vec<(Dim, Dim)> {
    // does not currently handle optional args, just required args
    let mut swaps = Vec::new();
    for datatype in schemas.keys() {
        let aliases = datatype.arg_aliases();
        if aliases.is_empty() {
            continue
        }
        for dim in datatype.required_parameters() {
            if args.dim_is_none(&dim) {
                for (k, v) in aliases.iter() {
                    if v == &dim && args.dim_is_some(k) {
                        swaps.push((*k, *v));
                    }
                }
            }
        }
    }
    swaps
}

trait DimIsNone {
    fn dim_is_some(&self, dim: &Dim) -> bool;
    fn dim_is_none(&self, dim: &Dim) -> bool;
}

impl DimIsNone for Args {
    fn dim_is_some(&self, dim: &Dim) -> bool {
        match dim {
            Dim::BlockNumber => self.blocks.is_some(),
            Dim::TransactionHash => self.txs.is_some(),
            Dim::Address => self.address.is_some(),
            Dim::ToAddress => self.to_address.is_some(),
            Dim::Contract => self.contract.is_some(),
            Dim::CallData => self.call_data.is_some(),
            Dim::Slot => self.slot.is_some(),
            Dim::Topic0 => self.topic0.is_some(),
            Dim::Topic1 => self.topic1.is_some(),
            Dim::Topic2 => self.topic2.is_some(),
            Dim::Topic3 => self.topic3.is_some(),
        }
    }

    fn dim_is_none(&self, dim: &Dim) -> bool {
        match dim {
            Dim::BlockNumber => self.blocks.is_none(),
            Dim::TransactionHash => self.txs.is_none(),
            Dim::Address => self.address.is_none(),
            Dim::ToAddress => self.to_address.is_none(),
            Dim::Contract => self.contract.is_none(),
            Dim::CallData => self.call_data.is_none(),
            Dim::Slot => self.slot.is_none(),
            Dim::Topic0 => self.topic0.is_none(),
            Dim::Topic1 => self.topic1.is_none(),
            Dim::Topic2 => self.topic2.is_none(),
            Dim::Topic3 => self.topic3.is_none(),
        }
    }
}

fn apply_arg_aliases(args: &Args, arg_aliases: Vec<(Dim, Dim)>) -> Result<Args, ParseError> {
    let mut args = (*args).clone();
    for (k, v) in arg_aliases.iter() {
        args = match (k, v) {
            (Dim::Contract, Dim::Address) => {
                Args { address: args.contract.clone(), contract: None, ..args }
            }
            (Dim::ToAddress, Dim::Address) => {
                Args { address: args.to_address.clone(), to_address: None, ..args }
            }
            (Dim::Address, Dim::Contract) => {
                Args { contract: args.address.clone(), address: None, ..args }
            }
            (Dim::ToAddress, Dim::Contract) => {
                Args { contract: args.to_address.clone(), to_address: None, ..args }
            }
            (Dim::Address, Dim::ToAddress) => {
                Args { to_address: args.address.clone(), address: None, ..args }
            }
            (Dim::Contract, Dim::ToAddress) => {
                Args { to_address: args.contract.clone(), contract: None, ..args }
            }
            _ => return Err(ParseError::ParseError("invalid arg alias pairing".to_string())),
        };
    }
    Ok(args)
}
