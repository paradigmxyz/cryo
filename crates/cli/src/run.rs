use crate::{args, parse};
use cryo_freeze::{CollectError, ExecutionEnv, FreezeSummary};
use std::{sync::Arc, time::SystemTime};

/// run cli
pub async fn run(args: args::Args) -> Result<Option<FreezeSummary>, CollectError> {
    if args.datatype.contains(&"datasets".to_string()) {
        cryo_freeze::print_all_datasets();
        return Ok(None)
    } else if args.datatype.first() == Some(&"info".to_string()) {
        let args = args::Args { datatype: args.datatype[1..].to_vec(), ..args };
        let (query, _, _, _) = match parse::parse_args(&args).await {
            Ok(opts) => opts,
            Err(e) => return Err(e.into()),
        };
        for (datatype, schema) in query.schemas.iter() {
            if query.schemas.len() > 1 {
                println!();
                println!();
            }
            cryo_freeze::print_dataset_info(*datatype, schema);
        }
        return Ok(None)
    }

    let t_start_parse = Some(SystemTime::now());
    let (query, source, sink, env) = match parse::parse_args(&args).await {
        Ok(opts) => opts,
        Err(e) => return Err(e.into()),
    };
    let source = Arc::new(source);
    let env = ExecutionEnv { t_start_parse, ..env };
    let env = env.set_start_time();
    cryo_freeze::freeze(&query, &source, &sink, &env).await
}
