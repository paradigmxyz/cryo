use crate::{args, parse, remember};
use clap_cryo::Parser;
use color_print::cstr;
use colored::Colorize;
use cryo_freeze::{CollectError, ExecutionEnv, FreezeSummary};
use std::{sync::Arc, time::SystemTime};

/// run cli
pub async fn run(args: args::Args) -> Result<Option<FreezeSummary>, CollectError> {
    // handle subcommands
    if args.datatype.first() == Some(&"help".to_string()) {
        return handle_help_subcommands(args).await
    }

    let cryo_dir: std::path::PathBuf = args.output_dir.clone().into();
    let cryo_dir = cryo_dir.join(".cryo");

    // remember previous command
    let args = if args.datatype.is_empty() {
        let remembered = remember::load_remembered_command(cryo_dir.clone())?;
        if remembered.cryo_version != cryo_freeze::CRYO_VERSION {
            eprintln!("remembered command comes from different cryo version, proceed with caution");
            eprintln!();
        };
        println!(
            "{} {} {}",
            "remembering previous command:".truecolor(170, 170, 170),
            "cryo".bold().white(),
            remembered.command.into_iter().skip(1).collect::<Vec<_>>().join(" ").white().bold()
        );
        println!();
        args.merge_with_precedence(remembered.args)
    } else {
        args
    };

    // remember current command
    if args.remember {
        println!("remembering this command for future use");
        println!();
        remember::save_remembered_command(cryo_dir, &args)?;
    }

    // handle regular flow
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

async fn handle_help_subcommands(args: args::Args) -> Result<Option<FreezeSummary>, CollectError> {
    if args.datatype.len() == 1 {
        args::Args::parse_from(vec!["cryo", "-h"]);
    } else if args.datatype.len() == 2 && args.datatype[1] == "syntax" {
        let content = cstr!(
            r#"<white><bold>Block specification syntax</bold></white>
- can use numbers                    <white><bold>--blocks 5000 6000 7000</bold></white>
- can use ranges                     <white><bold>--blocks 12M:13M 15M:16M</bold></white>
- can use a parquet file             <white><bold>--blocks ./path/to/file.parquet[:COLUMN_NAME]</bold></white>
- can use multiple parquet files     <white><bold>--blocks ./path/to/files/*.parquet[:COLUMN_NAME]</bold></white>
- numbers can contain { _ . K M B }  <white><bold>5_000 5K 15M 15.5M</bold></white>
- omitting range end means latest    <white><bold>15.5M:</bold></white> == <white><bold>15.5M:latest</bold></white>
- omitting range start means 0       <white><bold>:700</bold></white> == <white><bold>0:700</bold></white>
- minus on start means minus end     <white><bold>-1000:7000</bold></white> == <white><bold>6000:7000</bold></white>
- plus sign on end means plus start  <white><bold>15M:+1000</bold></white> == <white><bold>15M:15.001K</bold></white>
- can use every nth value            <white><bold>2000:5000:1000</bold></white> == <white><bold>2000 3000 4000</bold></white>
- can use n values total             <white><bold>100:200/5</bold></white> == <white><bold>100 124 149 174 199</bold></white>

<white><bold>Transaction specification syntax</bold></white>
- can use transaction hashes         <white><bold>--txs TX_HASH1 TX_HASH2 TX_HASH3</bold></white>
- can use a parquet file             <white><bold>--txs ./path/to/file.parquet[:COLUMN_NAME]</bold></white>
                                     (default column name is <white><bold>transaction_hash</bold></white>)
- can use multiple parquet files     <white><bold>--txs ./path/to/ethereum__logs*.parquet</bold></white>"#
        );
        println!("{}", content);
    } else if args.datatype.len() == 2 && args.datatype.contains(&"datasets".to_string()) {
        cryo_freeze::print_all_datasets();
    } else {
        let args = args::Args { datatype: args.datatype[1..].to_vec(), ..args };
        let schemas = super::parse::schemas::parse_schemas(&args)?;
        for (datatype, schema) in schemas.iter() {
            if schemas.len() > 1 {
                println!();
                println!();
            }
            cryo_freeze::print_dataset_info(*datatype, schema);
        }
    }
    Ok(None)
}
