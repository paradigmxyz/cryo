use std::collections::HashMap;

use thousands::Separable;
use std::time::SystemTime;
use chrono::{DateTime, Local};

use crate::cli::Args;
use crate::types::{Datatype, FreezeOpts, Schema};
use crate::block_utils;
use crate::output_utils::generic_outputs;


pub fn print_cryo_summary(opts: &FreezeOpts, args: &Args) {
    generic_outputs::print_header("cryo parameters");
    let datatype_strs: Vec<_> = opts.datatypes.iter().map(|d| d.as_str()).collect();
    generic_outputs::print_bullet("datatypes", datatype_strs.join(", "));
    generic_outputs::print_bullet("network", &opts.network_name);
    generic_outputs::print_bullet("provider", &args.rpc);
    generic_outputs::print_bullet(
        "total blocks",
        block_utils::get_total_blocks(&opts.block_chunks).to_string(),
    );
    generic_outputs::print_bullet("block chunk size", args.chunk_size.to_string());
    generic_outputs::print_bullet("total block chunks", opts.block_chunks.len().to_string());
    generic_outputs::print_bullet(
        "max concurrent chunks",
        opts.max_concurrent_chunks.to_string(),
    );
    generic_outputs::print_bullet(
        "max concurrent blocks",
        opts.max_concurrent_blocks.to_string(),
    );
    if opts.datatypes.contains(&Datatype::Logs) {
        generic_outputs::print_bullet("log request size", opts.log_request_size.to_string());
    };
    generic_outputs::print_bullet("output format", opts.output_format.as_str());
    generic_outputs::print_bullet("binary column format", opts.binary_column_format.as_str());
    generic_outputs::print_bullet("output dir", &opts.output_dir);
    print_schemas(&opts.schemas, &opts);
}

fn print_schemas(schemas: &HashMap<Datatype, Schema>, opts: &FreezeOpts) {
    schemas.iter().for_each(|(name, schema)| {
        println!("");
        println!("");
        print_schema(&name, &schema, opts.sort.get(name).unwrap().to_vec())
    })
}

fn print_schema(name: &Datatype, schema: &Schema, sort: Vec<String>) {
    generic_outputs::print_header("schema for ".to_string() + name.as_str());
    schema.iter().for_each(|(name, column_type)| {
        generic_outputs::print_bullet(name, column_type.as_str());
    });
    println!("");
    println!("sorting {} by: {}", name.as_str(), sort.join(", "));
}

pub fn print_cryo_conclusion(
    t_start: SystemTime,
    t_parse_done: SystemTime,
    t_data_done: SystemTime,
    opts: &FreezeOpts,
) {
    let dt_start: DateTime<Local> = t_start.into();
    let dt_data_done: DateTime<Local> = t_data_done.into();

    let duration = t_data_done.duration_since(t_start).unwrap();
    let seconds = duration.as_secs();
    let millis = duration.subsec_millis();
    let duration_string = format!("{}.{:03} seconds", seconds, millis);

    generic_outputs::print_header("collection summary");
    generic_outputs::print_bullet(
        "t_start",
        dt_start.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
    );
    generic_outputs::print_bullet(
        "t_end",
        "  ".to_string()
            + dt_data_done
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string()
                .as_str(),
    );
    generic_outputs::print_bullet("total duration", duration_string);
    let total_blocks = block_utils::get_total_blocks(&opts.block_chunks) as f64;
    let total_time = (seconds as f64) + (duration.subsec_nanos() as f64) / 1e9;
    let blocks_per_second = total_blocks / total_time;
    let blocks_per_minute = blocks_per_second * 60.0;
    let blocks_per_hour = blocks_per_minute * 60.0;
    let blocks_per_day = blocks_per_hour * 24.0;
    generic_outputs::print_bullet("blocks per second", format_float(blocks_per_second));
    generic_outputs::print_bullet("blocks per minute", format_float(blocks_per_minute));
    generic_outputs::print_bullet("blocks per hour", "  ".to_string() + format_float(blocks_per_hour).as_str());
    generic_outputs::print_bullet("blocks per day", "   ".to_string() + format_float(blocks_per_day).as_str());
}

fn format_float(number: f64) -> String {
    round_to_decimal_places(number, 1).separate_with_commas()
}

fn round_to_decimal_places(number: f64, dp: u32) -> f64 {
    let multiplier = 10f64.powi(dp as i32);
    (number * multiplier).round() / multiplier
}
