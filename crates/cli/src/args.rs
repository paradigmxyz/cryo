use clap_cryo::Parser;
use color_print::cstr;
use serde::{Deserialize, Serialize};

/// Command line arguments
#[derive(Parser, Debug, Serialize, Deserialize)]
#[command(name = "cryo", author, version, about = get_about_str(), long_about = None, styles=get_styles(), after_help=get_after_str(), allow_negative_numbers = true)]
pub struct Args {
    /// datatype to collect
    #[arg(required = true, help=get_datatype_help(), num_args(1..))]
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

    /// Columns to exclude from the default output
    #[arg(short, long, value_name="COLS", num_args(0..), help_heading="Content Options")]
    pub exclude_columns: Option<Vec<String>>,

    /// Columns to use instead of the default columns,
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

    /// Specify max retries on provider errors
    #[arg(long, default_value_t = 5, value_name = "R", help_heading = "Acquisition Options")]
    pub max_retries: u32,

    /// Specify initial backoff for retry strategy (ms)
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

    /// Run quietly without printing information to stdout
    #[arg(long)]
    pub no_verbose: bool,

    /// Number of blocks per file
    #[arg(short, long, default_value_t = 1000, help_heading = "Output Options")]
    pub chunk_size: u64,

    /// Number of files (alternative to --chunk-size)
    #[arg(long, help_heading = "Output Options")]
    pub n_chunks: Option<u64>,

    /// Directory for output files
    #[arg(short, long, default_value = ".", help_heading = "Output Options")]
    pub output_dir: String,

    /// Suffix to attach to end of each filename
    #[arg(long, help_heading = "Output Options")]
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
    pub report_dir: Option<String>,

    /// Avoid saving a summary report
    #[arg(long, help_heading = "Output Options")]
    pub no_report: bool,

    /// Address
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub address: Option<Vec<String>>,

    /// To Address
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..), value_name="TO")]
    pub to_address: Option<Vec<String>>,

    /// From Address
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..), value_name="FROM")]
    pub from_address: Option<Vec<String>>,

    /// [eth_calls] Call data to use for eth_calls
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub call_data: Option<Vec<String>>,

    /// [eth_calls] Function to use for eth_calls
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub function: Option<Vec<String>>,

    /// [eth_calls] Inputs to use for eth_calls
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub inputs: Option<Vec<String>>,

    /// [slots] Slots
    #[arg(long, help_heading = "Dataset-specific Options", num_args(1..))]
    pub slots: Option<Vec<String>>,

    /// [logs] filter logs by contract address
    #[arg(long, help_heading = "Dataset-specific Options")]
    pub contract: Option<Vec<String>>,

    /// [logs] filter logs by topic0
    #[arg(long, visible_alias = "event", help_heading = "Dataset-specific Options")]
    pub topic0: Option<String>,

    /// [logs] filter logs by topic1
    #[arg(long, help_heading = "Dataset-specific Options")]
    pub topic1: Option<String>,

    /// [logs] filter logs by topic2
    #[arg(long, help_heading = "Dataset-specific Options")]
    pub topic2: Option<String>,

    /// [logs] filter logs by topic3
    #[arg(long, help_heading = "Dataset-specific Options")]
    pub topic3: Option<String>,

    /// [logs] Blocks per request
    #[arg(
        long,
        value_name = "SIZE",
        default_value_t = 1,
        help_heading = "Dataset-specific Options"
    )]
    pub inner_request_size: u64,

    /// [logs] event signature to parse
    #[arg(long, value_name = "SIGNATURE", help_heading = "Dataset-specific Options")]
    pub event_signature: Option<String>,
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

fn get_about_str() -> &'static str {
    cstr!(r#"<white><bold>cryo</bold></white> extracts blockchain data to parquet, csv, or json"#)
}

fn get_after_str() -> &'static str {
    cstr!(
        r#"
<white><bold>Block specification syntax</bold></white>
- can use numbers                    <white><bold>--blocks 5000 6000 7000</bold></white>
- can use ranges                     <white><bold>--blocks 12M:13M 15M:16M</bold></white>
- numbers can contain { _ . K M B }  <white><bold>5_000 5K 15M 15.5M</bold></white>
- omiting range end means latest     <white><bold>15.5M:</bold></white> == <white><bold>15.5M:latest</bold></white>
- omitting range start means 0       <white><bold>:700</bold></white> == <white><bold>0:700</bold></white>
- minus on start means minus end     <white><bold>-1000:7000</bold></white> == <white><bold>6000:7000</bold></white>
- plus sign on end means plus start  <white><bold>15M:+1000</bold></white> == <white><bold>15M:15.001K</bold></white>

<white><bold>Transaction hash specification syntax</bold></white>
- can use transaction hashes         <white><bold>--txs TX_HASH1 TX_HASH2 TX_HASH3</bold></white>
- can use a parquet file             <white><bold>--txs ./path/to/file.parquet[:COLUMN_NAME]</bold></white>
                                     (default column name is <white><bold>transaction_hash</bold></white>)
- can use multiple parquet files     <white><bold>--txs ./path/to/ethereum__logs*.parquet</bold></white>
"#
    )
}

fn get_datatype_help() -> &'static str {
    cstr!(
        r#"datatype(s) to collect, one or more of:
- <white><bold>blocks</bold></white>
- <white><bold>transactions</bold></white>  (alias = <white><bold>txs</bold></white>)
- <white><bold>logs</bold></white>          (alias = <white><bold>events</bold></white>)
- <white><bold>contracts</bold></white>
- <white><bold>traces</bold></white>        (alias = <white><bold>call_traces</bold></white>)
- <white><bold>state_diffs</bold></white>   (= balance + code + nonce + storage diffs)
- <white><bold>balance_diffs</bold></white>
- <white><bold>code_diffs</bold></white>
- <white><bold>nonce_diffs</bold></white>
- <white><bold>storage_diffs</bold></white>
- <white><bold>vm_traces</bold></white>     (alias = <white><bold>opcode_traces</bold></white>)"#
    )
}
