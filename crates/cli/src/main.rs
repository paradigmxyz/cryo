mod cli;
mod summaries;

use std::time::SystemTime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let t_start = SystemTime::now();
    let opts = cli::parse_opts().await;
    let t_parse_done = SystemTime::now();
    summaries::print_cryo_summary(&opts);
    if opts.dry_run {
        println!("\n\n[dry run, exiting]");
    } else {
        summaries::print_header("\n\ncollecting data");
        let freeze_summary = cryo_freezer::freeze(opts.clone()).await.unwrap();
        let t_data_done = SystemTime::now();
        println!("...done\n\n");
        summaries::print_cryo_conclusion(t_start, t_parse_done, t_data_done, &opts, &freeze_summary);
    };
    Ok(())
}
