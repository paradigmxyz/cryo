use crate::{args, parse, remember};
use clap_cryo::Parser;
use color_print::cstr;
use colored::Colorize;
use cryo_freeze::{err, CollectError, ExecutionEnv, FreezeSummary};
use std::{sync::Arc, time::SystemTime};

/// Entry point to run the CLI application.
pub async fn run(args: args::Args) -> Result<Option<FreezeSummary>, CollectError> {
    if is_help_command(&args) {
        return handle_help_subcommands(args);
    }

    let cryo_dir = build_cryo_directory(std::path::Path::new(&args.output_dir));

    let args =
        if args.datatype.is_empty() { load_or_remember_command(args, &cryo_dir)? } else { args };

    if args.remember {
        println!("remembering this command for future use\n");
        remember::save_remembered_command(cryo_dir, &args)?;
    }

    // handle regular flow
    run_freeze_process(args).await
}

/// Check if the command is a help command.
fn is_help_command(args: &args::Args) -> bool {
    args.datatype.first() == Some(&"help".to_string())
}

/// Build the cryo directory path.
fn build_cryo_directory(output_dir: &std::path::Path) -> std::path::PathBuf {
    output_dir.join(".cryo")
}

/// Load a previously remembered command or return the current args.
fn load_or_remember_command(
    mut args: args::Args,
    cryo_dir: &std::path::Path,
) -> Result<args::Args, CollectError> {
    let remembered = remember::load_remembered_command(cryo_dir.to_path_buf())?;
    // Warn if the remembered command comes from a different Cryo version.
    if remembered.cryo_version != cryo_freeze::CRYO_VERSION {
        eprintln!("remembered command comes from a different Cryo version, proceed with caution\n");
    }
    print_remembered_command(&remembered.command);
    args = args.merge_with_precedence(remembered.args);
    Ok(args)
}

/// Print the remembered command to the console.
fn print_remembered_command(command: &[String]) {
    println!(
        "{} {} {}",
        "remembering previous command:".truecolor(170, 170, 170),
        "cryo".bold().white(),
        command.iter().skip(1).cloned().collect::<Vec<_>>().join(" ").white().bold()
    );
    println!();
}

/// Run the main freezing process with the provided arguments.
async fn run_freeze_process(args: args::Args) -> Result<Option<FreezeSummary>, CollectError> {
    let t_start_parse = Some(SystemTime::now());
    let (query, source, sink, env) = parse::parse_args(&args).await?;

    let source = Arc::new(source);
    let env = ExecutionEnv { t_start_parse, ..env }.set_start_time();

    cryo_freeze::freeze(&query, &source, &sink, &env).await
}

/// Handle help-related subcommands.
fn handle_help_subcommands(args: args::Args) -> Result<Option<FreezeSummary>, CollectError> {
    match args.datatype.len() {
        // if only "help" is provided, print general help
        1 => print_general_help(),
        // if "syntax help" is provided, print syntax help
        2 if args.datatype[1] == "syntax" => print_syntax_help(),
        // if "help datasets" is provided, print dataset information
        2 if args.datatype.contains(&"datasets".to_string()) => cryo_freeze::print_all_datasets(),
        // if "help <datatype>" is provided, print detailed help
        _ => handle_detailed_help(args)?,
    }
    Ok(None)
}

/// Print general help for the CLI tool.
fn print_general_help() {
    args::Args::parse_from(vec!["cryo", "-h"]);
}

/// Print syntax help for block and transaction specification.
fn print_syntax_help() {
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
    println!("{content}");
}

/// Handle detailed help by parsing schemas and printing dataset information.
fn handle_detailed_help(args: args::Args) -> Result<(), CollectError> {
    let args = args::Args { datatype: args.datatype[1..].to_vec(), ..args };
    let (datatypes, schemas) = super::parse::schemas::parse_schemas(&args)?;

    for datatype in datatypes.into_iter() {
        if schemas.len() > 1 {
            println!("\n");
        }
        if let Some(schema) = schemas.get(&datatype) {
            cryo_freeze::print_dataset_info(datatype, schema);
        } else {
            return Err(err(format!("missing schema for datatype: {datatype:?}").as_str()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::Args;

    #[test]
    fn test_handle_help_subcommands_general_help() {
        let args = Args { datatype: vec!["help".to_string()], ..Default::default() };

        let result = handle_help_subcommands(args);

        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_help_subcommands_syntax_help() {
        let args =
            Args { datatype: vec!["help".to_string(), "syntax".to_string()], ..Default::default() };

        let result = handle_help_subcommands(args);

        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_help_subcommands_datasets_help() {
        let args = Args {
            datatype: vec!["help".to_string(), "datasets".to_string()],
            ..Default::default()
        };

        let result = handle_help_subcommands(args);

        assert!(result.is_ok());
    }
}
