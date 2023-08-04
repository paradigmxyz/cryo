use std::time::SystemTime;

use crate::{args, parse, summaries};
use cryo_freeze::{FreezeError, FreezeSummary};

/// run freeze for given Args
pub async fn run(args: args::Args) -> Result<Option<FreezeSummary>, FreezeError> {
    // parse inputs
    let t_start = SystemTime::now();
    let (query, source, sink) = match parse::parse_opts(&args).await {
        Ok(opts) => opts,
        Err(e) => return Err(e.into()),
    };
    let t_parse_done = SystemTime::now();

    // print summary
    if !args.no_verbose {
        summaries::print_cryo_summary(&query, &source, &sink);
    }

    // check dry run
    if args.dry {
        if !args.no_verbose {
            println!("\n\n[dry run, exiting]");
        }
        return Ok(None)
    };

    // collect data
    if !args.no_verbose {
        summaries::print_header("\n\ncollecting data");
    }
    match cryo_freeze::freeze(&query, &source, &sink).await {
        Ok(freeze_summary) => {
            // print summary
            let t_data_done = SystemTime::now();
            if !args.no_verbose {
                println!("...done\n\n");
                summaries::print_cryo_conclusion(
                    t_start,
                    t_parse_done,
                    t_data_done,
                    &query,
                    &freeze_summary,
                )
            }

            // return summary
            Ok(Some(freeze_summary))
        }

        Err(e) => {
            println!("{}", e);
            Err(e)
        }
    }
}
