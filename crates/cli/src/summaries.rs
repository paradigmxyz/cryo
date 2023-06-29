use std::collections::HashMap;

use chrono::{DateTime, Local};
use colored::Colorize;
use std::time::SystemTime;
use thousands::Separable;

use cryo_freezer;
use cryo_freezer::Datatype;
use cryo_freezer::FreezeOpts;
use cryo_freezer::FreezeSummary;
use cryo_freezer::Schema;

const TITLE_R: u8 = 0;
const TITLE_G: u8 = 225;
const TITLE_B: u8 = 0;

pub fn print_header<A: AsRef<str>>(header: A) {
    let header_str = header.as_ref().white().bold();
    let underline = "─"
        .repeat(header_str.len())
        .truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!("{}", header_str);
    println!("{}", underline);
}

pub fn print_bullet<A: AsRef<str>, B: AsRef<str>>(key: A, value: B) {
    let bullet_str = "- ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    let key_str = key.as_ref().white().bold();
    let value_str = value.as_ref().truecolor(170, 170, 170);
    let colon_str = ": ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!("{}{}{}{}", bullet_str, key_str, colon_str, value_str);
}

pub fn print_cryo_summary(opts: &FreezeOpts) {
    print_header("cryo parameters");
    let datatype_strs: Vec<_> = opts.datatypes.iter().map(|d| d.dataset().name()).collect();
    print_bullet("datatypes", datatype_strs.join(", "));
    print_bullet("network", &opts.network_name);
    // let rpc_url = cli::parse_rpc_url(args);
    // print_bullet("provider", rpc_url);
    print_bullet(
        "min block",
        cryo_freezer::get_min_block(&opts.block_chunks).separate_with_commas(),
    );
    print_bullet(
        "max block",
        cryo_freezer::get_max_block(&opts.block_chunks).separate_with_commas(),
    );
    print_bullet(
        "total blocks",
        cryo_freezer::get_total_blocks(&opts.block_chunks).separate_with_commas(),
    );
    let chunk_size = cryo_freezer::get_total_blocks(&[opts.block_chunks.get(0).unwrap().clone()]);
    print_bullet("block chunk size", chunk_size.separate_with_commas());
    print_bullet(
        "total block chunks",
        opts.block_chunks.len().separate_with_commas(),
    );
    print_bullet(
        "max concurrent chunks",
        opts.max_concurrent_chunks.separate_with_commas(),
    );
    print_bullet(
        "max concurrent blocks",
        opts.max_concurrent_blocks.separate_with_commas(),
    );
    if opts.datatypes.contains(&Datatype::Logs) {
        print_bullet("log request size", opts.log_request_size.to_string());
    };
    print_bullet("output format", opts.output_format.as_str());
    print_bullet("binary column format", opts.binary_column_format.as_str());
    print_bullet("output dir", &opts.output_dir);
    print_schemas(&opts.schemas, opts);
}

pub fn print_schemas(schemas: &HashMap<Datatype, Schema>, opts: &FreezeOpts) {
    schemas.iter().for_each(|(name, schema)| {
        println!();
        println!();
        print_schema(name, schema, opts.sort.get(name).unwrap().to_vec())
    })
}

pub fn print_schema(name: &Datatype, schema: &Schema, sort: Vec<String>) {
    print_header("schema for ".to_string() + name.dataset().name());
    schema.iter().for_each(|(name, column_type)| {
        print_bullet(name, column_type.as_str());
    });
    println!();
    println!("sorting {} by: {}", name.dataset().name(), sort.join(", "));
}

pub fn print_cryo_conclusion(
    t_start: SystemTime,
    _t_parse_done: SystemTime,
    t_data_done: SystemTime,
    opts: &FreezeOpts,
    freeze_summary: &FreezeSummary,
) {
    let dt_start: DateTime<Local> = t_start.into();
    let dt_data_done: DateTime<Local> = t_data_done.into();

    let duration = t_data_done.duration_since(t_start).unwrap();
    let seconds = duration.as_secs();
    let millis = duration.subsec_millis();
    let duration_string = format!("{}.{:03} seconds", seconds, millis);

    print_header("collection summary");
    print_bullet(
        "t_start",
        dt_start.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
    );
    print_bullet(
        "t_end",
        "  ".to_string()
            + dt_data_done
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string()
                .as_str(),
    );
    print_bullet("total duration", duration_string);
    print_bullet(
        "chunks completed",
        freeze_summary.n_completed.separate_with_commas(),
    );
    print_bullet(
        "chunks skipped",
        freeze_summary.n_skipped.separate_with_commas(),
    );
    let total_blocks = cryo_freezer::get_total_blocks(&opts.block_chunks) as f64;
    let blocks_completed =
        total_blocks * (freeze_summary.n_completed as f64 / opts.block_chunks.len() as f64);
    print_bullet("blocks completed", blocks_completed.separate_with_commas());
    let total_time = (seconds as f64) + (duration.subsec_nanos() as f64) / 1e9;
    let blocks_per_second = blocks_completed / total_time;
    let blocks_per_minute = blocks_per_second * 60.0;
    let blocks_per_hour = blocks_per_minute * 60.0;
    let blocks_per_day = blocks_per_hour * 24.0;
    print_bullet("blocks per second", format_float(blocks_per_second));
    print_bullet("blocks per minute", format_float(blocks_per_minute));
    print_bullet(
        "blocks per hour",
        "  ".to_string() + format_float(blocks_per_hour).as_str(),
    );
    print_bullet(
        "blocks per day",
        "   ".to_string() + format_float(blocks_per_day).as_str(),
    );
}

fn format_float(number: f64) -> String {
    round_to_decimal_places(number, 1).separate_with_commas()
}

fn round_to_decimal_places(number: f64, dp: u32) -> f64 {
    let multiplier = 10f64.powi(dp as i32);
    (number * multiplier).round() / multiplier
}
