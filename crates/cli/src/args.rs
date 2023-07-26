use clap::Parser;
use color_print::cstr;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "cryo", author, version, about = get_about_str(), long_about = None, styles=get_styles(), after_help=get_after_str())]
pub struct Args {
    /// datatype to collect
    #[arg(required = true, help=get_datatype_help(), num_args(1..))]
    pub datatype: Vec<String>,

    /// Block numbers, see syntax below
    #[arg(
        short,
        long,
        default_value = "0:latest",
        allow_hyphen_values(true),
        help_heading = "Content Options"
    )]
    pub blocks: Vec<String>,

    /// Align block chunk boundaries to regular intervals,
    /// e.g. (1000, 2000, 3000) instead of (1106, 2106, 3106)
    #[arg(short, long, help_heading = "Content Options", verbatim_doc_comment)]
    pub align: bool,

    /// Reorg buffer, save blocks only when they are this old,
    /// can be a number of blocks
    #[arg(
        long,
        default_value_t = 0,
        value_name = "N_BLOCKS",
        help_heading = "Content Options",
        verbatim_doc_comment
    )]
    pub reorg_buffer: u64,

    // #[arg(
    //     short,
    //     long,
    //     allow_hyphen_values(true),
    //     help_heading = "Content Options",
    //     help = "Select by data transaction instead of by block,\ncan be a list or a file, see
    // syntax below", )]
    // pub txs: Vec<String>,
    /// Columns to include alongside the default output,
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

    /// Use hex string encoding for binary columns
    #[arg(long, help_heading = "Content Options")]
    pub hex: bool,

    /// Columns(s) to sort by, `none` to disable sorting
    #[arg(short, long, num_args(0..), help_heading="Content Options")]
    pub sort: Option<Vec<String>>,

    /// RPC url [default: ETH_RPC_URL env var]
    #[arg(short, long, help_heading = "Source Options")]
    pub rpc: Option<String>,

    /// Network name [default: use name of eth_getChainId]
    #[arg(long, help_heading = "Source Options")]
    pub network_name: Option<String>,

    /// Ratelimit on requests per second
    #[arg(short('l'), long, value_name = "limit", help_heading = "Acquisition Options")]
    pub requests_per_second: Option<u32>,

    /// Global number of concurrent requests
    #[arg(long, value_name = "M", help_heading = "Acquisition Options")]
    pub max_concurrent_requests: Option<u64>,

    /// Number of chunks processed concurrently
    #[arg(long, value_name = "M", help_heading = "Acquisition Options")]
    pub max_concurrent_chunks: Option<u64>,

    /// Dry run, collect no data
    #[arg(short, long, help_heading = "Acquisition Options")]
    pub dry: bool,

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

    /// Overwrite existing files instead of skipping them
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

    /// Set compression algorithm and level
    #[arg(long, help_heading="Output Options", value_name="NAME [#]", num_args(1..=2), default_value = "lz4")]
    pub compression: Vec<String>,

    // /// [transactions] track gas used by each transaction
    // #[arg(long, help_heading = "Dataset-specific Options")]
    // pub gas_used: bool,
    /// [logs] filter logs by contract address
    #[arg(long, help_heading = "Dataset-specific Options")]
    pub contract: Option<String>,

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

    /// [logs] Number of blocks per log request
    #[arg(
        long,
        value_name = "BLOCKS",
        default_value_t = 1,
        help_heading = "Dataset-specific Options"
    )]
    pub inner_request_size: u64,
}

pub(crate) fn get_styles() -> clap::builder::Styles {
    let white = anstyle::Color::Rgb(anstyle::RgbColor(255, 255, 255));
    let green = anstyle::Color::Rgb(anstyle::RgbColor(0, 225, 0));
    let grey = anstyle::Color::Rgb(anstyle::RgbColor(170, 170, 170));
    let title = anstyle::Style::new().bold().fg_color(Some(green));
    let arg = anstyle::Style::new().bold().fg_color(Some(white));
    let comment = anstyle::Style::new().fg_color(Some(grey));
    clap::builder::Styles::styled()
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
"#
    )
}

fn get_datatype_help() -> &'static str {
    cstr!(
        r#"datatype(s) to collect, one or more of:
- <white><bold>blocks</bold></white>
- <white><bold>transactions</bold></white>  (alias = <white><bold>txs</bold></white>)
- <white><bold>logs</bold></white>          (alias = <white><bold>events</bold></white>)
- <white><bold>traces</bold></white>        (alias = <white><bold>call_traces</bold></white>)
- <white><bold>state_diffs</bold></white>   (= balance + code + nonce + storage diffs)
- <white><bold>balance_diffs</bold></white>
- <white><bold>code_diffs</bold></white>
- <white><bold>nonce_diffs</bold></white>
- <white><bold>storage_diffs</bold></white>
- <white><bold>vm_traces</bold></white>     (alias = <white><bold>opcode_traces</bold></white>)"#
    )
}
