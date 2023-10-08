use std::collections::HashMap;

use chrono::{DateTime, Local};
use colored::Colorize;
use thousands::Separable;

use crate::{
    chunks::chunk_ops::ValueToString, ChunkData, ChunkStats, CollectError, ColumnType, Datatype,
    Dim, ExecutionEnv, FileOutput, Partition, Query, Source, Table,
};
use std::path::PathBuf;

const TITLE_R: u8 = 0;
const TITLE_G: u8 = 225;
const TITLE_B: u8 = 0;
const ERROR_R: u8 = 225;
const ERROR_G: u8 = 0;
const ERROR_B: u8 = 0;

/// summary of a freeze
#[derive(Debug, Default)]
pub struct FreezeSummary {
    /// partitions completed
    pub completed: Vec<Partition>,
    /// partitions skipped
    pub skipped: Vec<Partition>,
    /// partitions errored
    pub errored: Vec<(Option<Partition>, CollectError)>,
}

pub(crate) fn print_header<A: AsRef<str>>(header: A) {
    let header_str = header.as_ref().white().bold();
    let underline = "─".repeat(header_str.len()).truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!("{}", header_str);
    println!("{}", underline);
}

pub(crate) fn print_header_error<A: AsRef<str>>(header: A) {
    let header_str = header.as_ref().white().bold();
    let underline = "─".repeat(header_str.len()).truecolor(ERROR_R, ERROR_G, ERROR_B);
    println!("{}", header_str);
    println!("{}", underline);
}

fn print_bullet<A: AsRef<str>, B: AsRef<str>>(key: A, value: B) {
    let bullet_str = "- ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    let key_str = key.as_ref().white().bold();
    let value_str = value.as_ref().truecolor(170, 170, 170);
    let colon_str = ": ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!("{}{}{}{}", bullet_str, key_str, colon_str, value_str);
}

fn print_bullet_indent<A: AsRef<str>, B: AsRef<str>>(key: A, value: B, indent: usize) {
    let bullet_str = "- ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    let key_str = key.as_ref().white().bold();
    let value_str = value.as_ref().truecolor(170, 170, 170);
    let colon_str = ": ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!("{}{}{}{}{}", " ".repeat(indent), bullet_str, key_str, colon_str, value_str);
}

pub(crate) fn print_cryo_intro(
    query: &Query,
    source: &Source,
    sink: &FileOutput,
    env: &ExecutionEnv,
    n_chunks_remaining: u64,
) {
    print_header("cryo parameters");
    let datatype_strs: Vec<_> = query.schemas.keys().map(|d| d.name()).collect();
    print_bullet("datatypes", datatype_strs.join(", "));
    print_bullet("network", &sink.prefix);
    // let rpc_url = cli::parse_rpc_url(args);
    // print_bullet("provider", rpc_url);
    print_chunks(&query.partitions);
    print_bullet(
        "chunks to collect",
        format!(
            "{} / {}",
            n_chunks_remaining.separate_with_commas(),
            query.partitions.len().separate_with_commas()
        ),
    );
    print_bullet("max concurrent chunks", source.max_concurrent_chunks.separate_with_commas());
    if query.schemas.contains_key(&Datatype::Logs) {
        print_bullet("inner request size", source.inner_request_size.to_string());
    };
    print_bullet("output format", sink.format.as_str());
    print_bullet("output dir", sink.output_dir.to_string_lossy());

    // print report path
    let report_path = if env.report && n_chunks_remaining > 0 {
        match super::reports::get_report_path(env, sink, true) {
            Ok(report_path) => {
                let stripped_path: PathBuf = match report_path.strip_prefix("./") {
                    Ok(stripped) => PathBuf::from(stripped),
                    Err(_) => report_path,
                };
                Some(stripped_path)
            }
            _ => None,
        }
    } else {
        None
    };
    match report_path {
        None => print_bullet("report file", "None"),
        Some(path) => print_bullet("report file", path.to_str().unwrap_or("none")),
    };

    // print schemas
    print_schemas(&query.schemas);

    if env.dry {
        println!("\n\n[dry run, exiting]");
    }

    println!();
    println!();
    print_header("collecting data")
}

fn print_chunks(chunks: &[Partition]) {
    let stats = crate::types::partitions::meta_chunks_stats(chunks);
    for (dim, dim_stats) in [("block", stats.block_numbers)].iter() {
        if let Some(dim_stats) = dim_stats {
            print_chunk(dim, dim_stats)
        }
    }

    for (dim, dim_stats) in vec![
        ("transaction", stats.transactions),
        ("call_data", stats.call_datas),
        ("address", stats.addresses),
        ("contract", stats.contracts),
        ("to_address", stats.to_addresses),
        ("slot", stats.slots),
        ("topic0", stats.topic0s),
        ("topic1", stats.topic1s),
        ("topic2", stats.topic2s),
        ("topic3", stats.topic3s),
    ]
    .iter()
    {
        if let Some(dim_stats) = dim_stats {
            print_chunk(dim, dim_stats)
        }
    }
}

fn print_chunk<T: Ord + ValueToString>(dim: &str, dim_stats: &ChunkStats<T>) {
    if dim_stats.total_values == 1 {
        print_bullet(dim, dim_stats.min_value_to_string().unwrap_or("none".to_string()));
    } else {
        print_bullet(format!("{} values", dim), "");
        if let Some(min_value_string) = dim_stats.min_value_to_string() {
            print_bullet_indent("min", min_value_string, 4);
        };
        if let Some(max_value_string) = dim_stats.max_value_to_string() {
            print_bullet_indent("max", max_value_string, 4);
        };
        print_bullet_indent("n_values", dim_stats.total_values.to_string(), 4);
        print_bullet_indent("n_chunks", dim_stats.n_chunks.to_string(), 4);
        print_bullet_indent("chunk size", dim_stats.chunk_size.to_string(), 4);
    }
}

fn print_schemas(schemas: &HashMap<Datatype, Table>) {
    schemas.iter().for_each(|(name, schema)| {
        println!();
        println!();
        print_schema(name, &schema.clone())
    })
}

fn print_schema(name: &Datatype, schema: &Table) {
    print_header("schema for ".to_string() + name.name().as_str());
    for column in schema.columns() {
        if let Some(column_type) = schema.column_type(column) {
            if column_type == ColumnType::UInt256 {
                for uint256_type in schema.u256_types.iter() {
                    print_bullet(
                        column.to_owned() + uint256_type.suffix().as_str(),
                        uint256_type.to_columntype().as_str(),
                    );
                }
            } else {
                print_bullet(column, column_type.as_str());
            }
        }
    }
    println!();
    if let Some(sort_cols) = schema.sort_columns.clone() {
        println!("sorting {} by: {}", name.name(), sort_cols.join(", "));
    } else {
        println!("sorting disabled for {}", name.name());
    }
    let other_columns =
        name.column_types().keys().copied().filter(|x| !schema.has_column(x)).collect::<Vec<_>>();
    let other_columns =
        if other_columns.is_empty() { "[none]".to_string() } else { other_columns.join(", ") };
    println!("\nother available columns: {}", other_columns);
}

pub(crate) fn print_cryo_conclusion(
    freeze_summary: &FreezeSummary,
    query: &Query,
    env: &ExecutionEnv,
) {
    if freeze_summary.errored.is_empty() {
        println!("...done")
    } else {
        println!("...done (errors in {} chunks)", freeze_summary.errored.len())
    };
    println!();
    println!();

    if !freeze_summary.errored.is_empty() {
        print_header_error("error summary");
        let mut error_counts: HashMap<String, usize> = HashMap::new();
        for (_partition, error) in freeze_summary.errored.iter() {
            *error_counts.entry(error.to_string()).or_insert(0) += 1;
        }
        for (error, count) in error_counts.iter().take(2) {
            println!("- {} ({}x)", error, count);
        }
        if error_counts.len() > 20 {
            println!("...")
        }
        println!();
        println!();
    }

    let new_env = match env.t_end {
        None => Some(env.clone().set_end_time()),
        Some(_) => None,
    };
    let env: &ExecutionEnv = match &new_env {
        Some(e) => e,
        None => env,
    };

    let dt_start: DateTime<Local> = env.t_start.into();
    let t_end = match env.t_end {
        Some(t_end) => t_end,
        _ => return,
    };
    let dt_data_done: DateTime<Local> = t_end.into();
    let duration = match t_end.duration_since(env.t_start) {
        Ok(duration) => duration,
        Err(_e) => {
            println!("error computing system time, aborting");
            return
        }
    };
    let seconds = duration.as_secs();
    let millis = duration.subsec_millis();
    let total_time = (seconds as f64) + (duration.subsec_nanos() as f64) / 1e9;
    let duration_string = format!("{}.{:03} seconds", seconds, millis);

    print_header("collection summary");
    print_bullet("total duration", duration_string);
    print_bullet("t_start", dt_start.format("%Y-%m-%d %H:%M:%S%.3f").to_string());
    print_bullet(
        "t_end",
        "  ".to_string() + dt_data_done.format("%Y-%m-%d %H:%M:%S%.3f").to_string().as_str(),
    );
    let n_chunks = query.partitions.len();
    print_bullet(
        "chunks errored",
        format!("  {} / {}", freeze_summary.errored.len().separate_with_commas(), n_chunks),
    );
    print_bullet(
        "chunks skipped",
        format!("  {} / {}", freeze_summary.skipped.len().separate_with_commas(), n_chunks),
    );
    print_bullet(
        "chunks collected",
        format!("{} / {}", freeze_summary.completed.len().separate_with_commas(), n_chunks),
    );

    print_chunks_speeds(query.partitions.clone(), &query.partitioned_by, total_time);
}

macro_rules! print_dim_speed {
    ($chunks:expr, $partition_by:expr, $total_time:expr, $name:ident, $dim:expr) => {
        if $partition_by.contains(&$dim) {
            print_chunk_speed(
                $dim.plural_name(),
                $total_time,
                $chunks.iter().map(|c| c.$name.clone()).collect(),
            );
        };
    };
}

fn print_chunks_speeds(chunks: Vec<Partition>, partition_by: &[Dim], total_time: f64) {
    print_dim_speed!(chunks, partition_by, total_time, block_numbers, Dim::BlockNumber);
    print_dim_speed!(chunks, partition_by, total_time, transactions, Dim::TransactionHash);
    print_dim_speed!(chunks, partition_by, total_time, call_datas, Dim::CallData);
    print_dim_speed!(chunks, partition_by, total_time, addresses, Dim::Address);
    print_dim_speed!(chunks, partition_by, total_time, contracts, Dim::Contract);
    print_dim_speed!(chunks, partition_by, total_time, slots, Dim::Slot);
    print_dim_speed!(chunks, partition_by, total_time, topic0s, Dim::Topic0);
    print_dim_speed!(chunks, partition_by, total_time, topic1s, Dim::Topic1);
    print_dim_speed!(chunks, partition_by, total_time, topic2s, Dim::Topic2);
    print_dim_speed!(chunks, partition_by, total_time, topic3s, Dim::Topic3);
}

fn print_chunk_speed<T: ChunkData>(name: &str, total_time: f64, chunks: Vec<Option<Vec<T>>>) {
    let flat_chunks: Vec<T> = chunks.into_iter().flatten().flatten().collect();
    let n_completed = flat_chunks.size();
    print_unit_speeds(name.into(), n_completed, total_time);
}
fn print_unit_speeds(name: String, n_completed: u64, total_time: f64) {
    let per_second = (n_completed as f64) / total_time;
    let per_minute = per_second * 60.0;
    let per_hour = per_minute * 60.0;
    let per_day = per_hour * 24.0;
    print_bullet("total ".to_string() + name.as_str(), n_completed.separate_with_commas());
    print_bullet_indent(name.clone() + " per second", format_float(per_second), 4);
    print_bullet_indent(name.clone() + " per minute", format_float(per_minute), 4);
    print_bullet_indent(
        name.clone() + " per hour",
        "  ".to_string() + format_float(per_hour).as_str(),
        4,
    );
    print_bullet_indent(name + " per day", "   ".to_string() + format_float(per_day).as_str(), 4);
}

fn format_float(number: f64) -> String {
    round_to_decimal_places(number, 1).separate_with_commas()
}

fn round_to_decimal_places(number: f64, dp: u32) -> f64 {
    let multiplier = 10f64.powi(dp as i32);
    (number * multiplier).round() / multiplier
}
