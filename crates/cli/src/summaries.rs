use std::collections::HashMap;

use chrono::{DateTime, Local};
use colored::Colorize;
use std::time::SystemTime;
use thousands::Separable;

use cryo_freeze::{
    BlockChunk, Chunk, ChunkData, Datatype, FileOutput, FreezeSummary, MultiQuery, Source, Table,
};

const TITLE_R: u8 = 0;
const TITLE_G: u8 = 225;
const TITLE_B: u8 = 0;

pub(crate) fn print_header<A: AsRef<str>>(header: A) {
    let header_str = header.as_ref().white().bold();
    let underline = "─".repeat(header_str.len()).truecolor(TITLE_R, TITLE_G, TITLE_B);
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

pub(crate) fn print_cryo_summary(query: &MultiQuery, source: &Source, sink: &FileOutput) {
    print_header("cryo parameters");
    let datatype_strs: Vec<_> = query.schemas.keys().map(|d| d.dataset().name()).collect();
    print_bullet("datatypes", datatype_strs.join(", "));
    print_bullet("network", &sink.prefix);
    // let rpc_url = cli::parse_rpc_url(args);
    // print_bullet("provider", rpc_url);
    let block_chunks = query
        .chunks
        .iter()
        .filter_map(|x| match x.clone() {
            Chunk::Block(chunk) => Some(chunk),
            _ => None,
        })
        .collect();
    print_block_chunks(block_chunks);
    print_bullet("max concurrent chunks", source.max_concurrent_chunks.separate_with_commas());
    if query.schemas.contains_key(&Datatype::Logs) {
        print_bullet("inner request size", source.inner_request_size.to_string());
    };
    print_bullet("output format", sink.format.as_str());
    print_bullet("output dir", &sink.output_dir);
    print_schemas(&query.schemas);
}

fn print_block_chunks(chunks: Vec<BlockChunk>) {
    if let Some(min) = chunks.min_value() {
        print_bullet("min block", min.separate_with_commas());
    };
    if let Some(max) = chunks.max_value() {
        print_bullet("max block", max.separate_with_commas());
    };
    print_bullet("total blocks", chunks.size().separate_with_commas());

    if let Some(first_chunk) = chunks.get(0) {
        let chunk_size = first_chunk.size();
        print_bullet("block chunk size", chunk_size.separate_with_commas());
    };
    print_bullet("total block chunks", chunks.len().separate_with_commas());
}

fn print_schemas(schemas: &HashMap<Datatype, Table>) {
    schemas.iter().for_each(|(name, schema)| {
        println!();
        println!();
        print_schema(name, &schema.clone())
    })
}

fn print_schema(name: &Datatype, schema: &Table) {
    print_header("schema for ".to_string() + name.dataset().name());
    for column in schema.columns() {
        if let Some(column_type) = schema.column_type(column) {
            print_bullet(column, column_type.as_str());
        }
    }
    println!();
    if let Some(sort_cols) = schema.sort_columns.clone() {
        println!("sorting {} by: {}", name.dataset().name(), sort_cols.join(", "));
    } else {
        println!("sorting disabled for {}", name.dataset().name());
    }
    let other_columns: String = name
        .dataset()
        .column_types()
        .keys()
        .copied()
        .filter(|x| !schema.has_column(x))
        .collect::<Vec<_>>()
        .join(", ");
    println!("\nother available columns: {}", other_columns);
}

pub(crate) fn print_cryo_conclusion(
    t_start: SystemTime,
    _t_parse_done: SystemTime,
    t_data_done: SystemTime,
    query: &MultiQuery,
    freeze_summary: &FreezeSummary,
) {
    let dt_start: DateTime<Local> = t_start.into();
    let dt_data_done: DateTime<Local> = t_data_done.into();

    let duration = match t_data_done.duration_since(t_start) {
        Ok(duration) => duration,
        Err(_e) => {
            println!("error computing system time, aborting");
            return
        }
    };
    let seconds = duration.as_secs();
    let millis = duration.subsec_millis();
    let duration_string = format!("{}.{:03} seconds", seconds, millis);

    print_header("collection summary");
    print_bullet("total duration", duration_string);
    print_bullet("t_start", dt_start.format("%Y-%m-%d %H:%M:%S%.3f").to_string());
    print_bullet(
        "t_end",
        "  ".to_string() + dt_data_done.format("%Y-%m-%d %H:%M:%S%.3f").to_string().as_str(),
    );
    let block_chunks: Vec<BlockChunk> = query
        .chunks
        .iter()
        .filter_map(|x| match x {
            Chunk::Block(chunk) => Some(chunk.clone()),
            _ => None,
        })
        .collect();
    let n_chunks = block_chunks.len();
    print_bullet("chunks errored", freeze_summary.n_errored.separate_with_commas());
    print_bullet("chunks skipped", freeze_summary.n_skipped.separate_with_commas());
    print_bullet(
        "chunks collected",
        format!("{} / {}", freeze_summary.n_completed.separate_with_commas(), n_chunks),
    );
    let total_blocks = block_chunks.size() as f64;
    let blocks_completed =
        total_blocks * (freeze_summary.n_completed as f64 / block_chunks.len() as f64);
    print_bullet("blocks collected", blocks_completed.separate_with_commas());
    let total_time = (seconds as f64) + (duration.subsec_nanos() as f64) / 1e9;
    let blocks_per_second = blocks_completed / total_time;
    let blocks_per_minute = blocks_per_second * 60.0;
    let blocks_per_hour = blocks_per_minute * 60.0;
    let blocks_per_day = blocks_per_hour * 24.0;
    print_bullet("blocks per second", format_float(blocks_per_second));
    print_bullet("blocks per minute", format_float(blocks_per_minute));
    print_bullet("blocks per hour", "  ".to_string() + format_float(blocks_per_hour).as_str());
    print_bullet("blocks per day", "   ".to_string() + format_float(blocks_per_day).as_str());
}

fn format_float(number: f64) -> String {
    round_to_decimal_places(number, 1).separate_with_commas()
}

fn round_to_decimal_places(number: f64, dp: u32) -> f64 {
    let multiplier = 10f64.powi(dp as i32);
    (number * multiplier).round() / multiplier
}
