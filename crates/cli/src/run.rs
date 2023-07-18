use eyre::Result;
use std::time::SystemTime;

use crate::args;
use crate::parse;
use crate::summaries;

/// run freeze for given Args
pub async fn run(args: args::Args) -> Result<()> {
    let t_start = SystemTime::now();
    let opts = match parse::parse_opts(args).await {
        Ok(opts) => opts,
        Err(e) => return Err(e),
    };
    let t_parse_done = SystemTime::now();
    summaries::print_cryo_summary(&opts);
    if opts.dry_run {
        println!("\n\n[dry run, exiting]");
    } else {
        summaries::print_header("\n\ncollecting data");
        match cryo_freeze::freeze(opts.clone()).await {
            Ok(freeze_summary) => {
                let t_data_done = SystemTime::now();
                println!("...done\n\n");
                summaries::print_cryo_conclusion(
                    t_start,
                    t_parse_done,
                    t_data_done,
                    &opts,
                    &freeze_summary,
                );
            }
            Err(e) => {
                println!("{}", e)
            }
        }
    };
    Ok(())
}
