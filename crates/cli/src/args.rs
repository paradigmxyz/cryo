use clap_cryo::Parser;
use color_print::cstr;
use colored::Colorize;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{default::Default, path::PathBuf};

/// Command line arguments
#[derive(Parser, Debug, Serialize, Deserialize, Clone, Default)]
#[command(
    name = "cryo",
    author,
    version = cryo_freeze::CRYO_VERSION,
    about = &get_about_str(),
    long_about = None,
    styles=get_styles(),
    after_help=&get_after_str(),
    allow_negative_numbers = true,
)]
pub struct Args {
    /// Datatype to collect
    #[arg(help=get_datatype_help(), num_args(0..))]
    pub datatype: Vec<String>,

    /// Block numbers, see syntax below
    #[arg(short, long, allow_negative_numbers = true, help_heading = "Content Options", num_args(1..))]
    pub blocks: Option<Vec<String>>,

    /// Transaction hashes, see syntax below
    #[arg(
        short,
        long,
        help_heading = "Content Options",
        num_args(1..),
    )]
    pub txs: Option<Vec<String>>,

    /// Align chunk boundaries to regular intervals,
    /// e.g. (1000 2000 3000), not (1106 2106 3106)
    #[arg(short, long, help_heading = "Content Options", verbatim_doc_comment)]
    pub align: bool,

    /// Reorg buffer, save blocks only when this old,
    /// can be a number of blocks
    #[arg(
        long,
        default_value_t = 0,
        value_name = "N_BLOCKS",
        help_heading = "Content Options",
        verbatim_doc_comment
    )]
    pub reorg_buffer: u64,

    /// Columns to include alongside the defaults,
    /// use `all` to include all available columns
    #[arg(short, long, value_name="COLS", num_args(0..), verbatim_doc_comment, help_heading="Content Options")]
    pub include_columns: Option<Vec<String>>,

    /// Columns to exclude from the defaults
    #[arg(short, long, value_name="COLS", num_args(0..), help_heading="Content Options")]
    pub exclude_columns: Option<Vec<String>>,

    /// Columns to use instead of the defaults,
    /// use `all` to use all available columns
    #[arg(long, value_name="COLS", num_args(0..), verbatim_doc_comment, help_heading="Content Options")]
    pub columns: Option<Vec<String>>,

    /// Set output datatype(s) of U256 integers
    /// [default: binary, string, f64]
    #[arg(long, num_args(1..), help_heading = "Content Options", verbatim_doc_comment)]
    pub u256_types: Option<Vec<String>>,

    /// Use hex string encoding for binary columns
    #[arg(long, help_heading = "Content Options")]
    pub hex: bool,

    /// Columns(s) to sort by, `none` for unordered
    #[arg(short, long, num_args(0..), help_heading="Content Options")]
    pub sort: Option<Vec<String>>,

    /// RPC url [default: ETH_RPC_URL env var]
    #[arg(short, long, help_heading = "Source Options")]
    pub rpc: Option<String>,

    /// Network name [default: name of eth_getChainId]
    #[arg(long, help_heading = "Source Options")]
    pub network_name: Option<String>,

    /// Ratelimit on requests per second
    #[arg(short('l'), long, value_name = "limit", help_heading = "Acquisition Options")]
    pub requests_per_second: Option<u32>,

    /// Max retries for provider errors
    #[arg(long, default_value_t = 5, value_name = "R", help_heading = "Acquisition Options")]
    pub max_retries: u32,

    /// Initial retry backoff time (ms)
    #[arg(long, default_value_t = 500, value_name = "B", help_heading = "Acquisition Options")]
    pub initial_backoff: u64,

    /// Global number of concurrent requests
    #[arg(long, value_name = "M", help_heading = "Acquisition Options")]
    pub max_concurrent_requests: Option<u64>,

    /// Number of chunks processed concurrently
    #[arg(long, value_name = "M", help_heading = "Acquisition Options")]
    pub max_concurrent_chunks: Option<u64>,

    /// Dry run, collect no data
    #[arg(short, long, help_heading = "Acquisition Options")]
    pub dry: bool,

    /// Remember current command for future use
    #[arg(long)]
    pub remember: bool,

    /// Extra verbosity
    #[arg(short, long)]
    pub verbose: bool,

    /// Run quietly without printing information to stdout
    #[arg(long)]
    pub no_verbose: bool,

    /// Number of blocks per file
    #[arg(short, long, default_value_t = 1000, help_heading = "Output Options")]
    pub chunk_size: u64,

    /// Number of files (alternative to --chunk-size)
    #[arg(long, help_heading = "Output Options")]
    pub n_chunks: Option<u64>,

    /// Dimensions to partition by
    #[arg(long, help_heading = "Output Options")]
    pub partition_by: Option<Vec<String>>,

    /// Directory for output files
    #[arg(short, long, default_value = ".", help_heading = "Output Options")]
    pub output_dir: String,

    /// Subdirectories for output files
    /// can be `datatype`, `network`, or custom string
    #[arg(long, help_heading = "Output Options", verbatim_doc_comment, num_args(1..))]
    pub subdirs: Vec<String>,

    /// Suffix to attach to end of each filename
    #[arg(long, help_heading = "Output Options", hide = true)]
    pub file_suffix: Option<String>,

    /// Overwrite existing files instead of skipping
    #[arg(long, help_heading = "Output Options")]
    pub overwrite: bool,

    /// Save as csv instead of parquet
    #[arg(long, help_heading = "Output Options")]
    pub csv: bool,

    /// Save as json instead of parquet
    #[arg(long, help_heading = "Output Options")]
    pub json: bool,

    /// Number of rows per row group in parquet file
    #[arg(long, value_name = "GROUP_SIZE", help_heading = "Output Options")]
    pub row_group_size: Option<usize>,

    /// Number of rows groups in parquet file
    #[arg(long, help_heading = "Output Options")]
    pub n_row_groups: Option<usize>,

    /// Do not write statistics to parquet files
    #[arg(long, help_heading = "Output Options")]
    pub no_stats: bool,

    /// Compression algorithm and level
    #[arg(long, help_heading="Output Options", value_name="NAME [#]", num_args(1..=2), default_value = "lz4")]
    pub compression: Vec<String>,

    /// Directory to save summary report
    /// [default: {output_dir}/.cryo/reports]
    #[arg(long, help_heading = "Output Options", verbatim_doc_comment)]
    pub report_dir: Option<PathBuf>,

    /// Avoid saving a summary report
    #[arg(long, help_heading = "Output Options")]
    pub no_report: bool,

    /// Address(es)
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub address: Option<Vec<String>>,

    /// To Address(es)
    #[arg(long, help_heading = "Dataset-specific Options", value_name="address", num_args(1..))]
    pub to_address: Option<Vec<String>>,

    /// From Address(es)
    #[arg(long, help_heading = "Dataset-specific Options", value_name="address", num_args(1..))]
    pub from_address: Option<Vec<String>>,

    /// Call data(s) to use for eth_calls
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub call_data: Option<Vec<String>>,

    /// Function(s) to use for eth_calls
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub function: Option<Vec<String>>,

    /// Input(s) to use for eth_calls
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub inputs: Option<Vec<String>>,

    /// Slot(s)
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub slot: Option<Vec<String>>,

    /// Contract address(es)
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub contract: Option<Vec<String>>,

    /// Topic0(s)
    #[arg(long, visible_alias = "event", help_heading = "Dataset-specific Options", num_args(1..))]
    pub topic0: Option<Vec<String>>,

    /// Topic1(s)
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub topic1: Option<Vec<String>>,

    /// Topic2(s)
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub topic2: Option<Vec<String>>,

    /// Topic3(s)
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub topic3: Option<Vec<String>>,

    /// Event signature for log decoding
    #[arg(long, value_name = "SIG", help_heading = "Dataset-specific Options", num_args(1..))]
    pub event_signature: Option<String>,

    /// Blocks per request (eth_getLogs)
    #[arg(
        long,
        value_name = "BLOCKS",
        default_value_t = 1,
        help_heading = "Dataset-specific Options"
    )]
    pub inner_request_size: u64,
}

impl Args {
    pub(crate) fn merge_with_precedence(self, other: Args) -> Self {
        let default_struct = Args::default();

        let mut s1_value: Value = serde_json::to_value(&self).expect("Failed to serialize to JSON");
        let s2_value: Value = serde_json::to_value(&other).expect("Failed to serialize to JSON");
        let default_value: Value =
            serde_json::to_value(&default_struct).expect("Failed to serialize to JSON");

        if let (Value::Object(s1_map), Value::Object(s2_map), Value::Object(default_map)) =
            (&mut s1_value, &s2_value, &default_value)
        {
            for (k, v) in s2_map.iter() {
                // If the value in s2 is different from the default, overwrite the value in s1
                if default_map.get(k) != Some(v) {
                    s1_map.insert(k.clone(), v.clone());
                }
            }
        }

        serde_json::from_value(s1_value).expect("Failed to deserialize from JSON")
    }
}

pub(crate) fn get_styles() -> clap_cryo::builder::Styles {
    let white = anstyle::Color::Rgb(anstyle::RgbColor(255, 255, 255));
    let green = anstyle::Color::Rgb(anstyle::RgbColor(0, 225, 0));
    let grey = anstyle::Color::Rgb(anstyle::RgbColor(170, 170, 170));
    let title = anstyle::Style::new().bold().fg_color(Some(green));
    let arg = anstyle::Style::new().bold().fg_color(Some(white));
    let comment = anstyle::Style::new().fg_color(Some(grey));
    clap_cryo::builder::Styles::styled()
        .header(title)
        .error(comment)
        .usage(title)
        .literal(arg)
        .placeholder(comment)
        .valid(title)
        .invalid(comment)
}

fn get_about_str() -> String {
    cstr!(r#"<white><bold>cryo</bold></white> extracts blockchain data to parquet, csv, or json"#)
        .to_string()
}

fn get_after_str() -> String {
    let header = "Optional Subcommands:".truecolor(0, 225, 0).bold().to_string();
    let subcommands = cstr!(
        r#"
      <white><bold>cryo help</bold></white>                      display help message
      <white><bold>cryo help syntax</bold></white>               display block + tx specification syntax
      <white><bold>cryo help datasets</bold></white>             display list of all datasets
      <white><bold>cryo help</bold></white>"#
    );
    let post_subcommands = " <DATASET(S)>         display info about a dataset";
    format!("{}{}{}", header, subcommands, post_subcommands)
}

fn get_datatype_help() -> &'static str {
    cstr!(
        r#"datatype(s) to collect, use <white><bold>cryo datasets</bold></white> to see all available"#
    )
}
