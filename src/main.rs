mod block_utils;
mod cli;
mod dataframes;
mod datatype_utils;
mod freeze;
mod gather;
mod output_utils;
mod types;

use std::time::SystemTime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let t_start = SystemTime::now();
    let (opts, args) = cli::parse_opts().await;
    let t_parse_done = SystemTime::now();
    output_utils::print_cryo_summary(&opts, &args);
    if opts.dry_run {
        println!("\n\n[dry run, exiting]");
    } else {
        output_utils::print_header("\n\ncollecting data");
        freeze::freeze(opts.clone()).await?;
        let t_data_done = SystemTime::now();
        println!("...done\n\n");
        output_utils::print_cryo_conclusion(t_start, t_parse_done, t_data_done, &opts);
    };
    Ok(())
}

