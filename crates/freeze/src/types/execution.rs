use crate::CollectError;
use indicatif::ProgressBar;
use std::{path::PathBuf, sync::Arc, time::SystemTime};

/// configuration of execution environment
#[derive(Clone)]
pub struct ExecutionEnv {
    /// dry run
    pub dry: bool,
    /// verbose output
    pub verbose: u32,
    /// whether to generate report
    pub report: bool,
    /// progress bar
    pub bar: Option<Arc<ProgressBar>>,
    /// cli command
    pub cli_command: Option<Vec<String>>,
    /// input args
    pub args: Option<String>,
    /// initial startup time
    pub t_start_parse: Option<SystemTime>,
    /// start time
    pub t_start: SystemTime,
    /// end time
    pub t_end: Option<SystemTime>,
    /// report directory
    pub report_dir: Option<PathBuf>,
}

impl ExecutionEnv {
    /// set start time
    pub fn set_start_time(self) -> Self {
        ExecutionEnv { t_start: SystemTime::now(), ..self }
    }

    /// set end time
    pub fn set_end_time(self) -> Self {
        ExecutionEnv { t_end: Some(SystemTime::now()), ..self }
    }
}

impl Default for ExecutionEnv {
    fn default() -> Self {
        ExecutionEnvBuilder::new().build()
    }
}

fn new_bar(n: u64) -> Result<Arc<ProgressBar>, CollectError> {
    let bar = Arc::new(ProgressBar::new(n));
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{wide_msg} ⏳ = {eta_precise} \n{wide_bar:.green}  {human_pos} / {human_len}  ⌛ = {elapsed_precise} ")
            .map_err(|_| CollectError::CollectError("error creating progress bar".to_string()))?,
    );
    Ok(bar)
}

/// build ExecutionEnv using builder pattern
pub struct ExecutionEnvBuilder {
    dry: bool,
    verbose: u32,
    report: bool,
    bar: Option<Arc<ProgressBar>>,
    cli_command: Option<Vec<String>>,
    args: Option<String>,
    t_start_parse: Option<SystemTime>,
    t_start: SystemTime,
    t_end: Option<SystemTime>,
    report_dir: Option<PathBuf>,
}

impl Default for ExecutionEnvBuilder {
    fn default() -> Self {
        ExecutionEnvBuilder {
            dry: false,
            verbose: 1,
            report: true,
            bar: None,
            cli_command: Some(std::env::args().collect()),
            args: None,
            t_start_parse: None,
            t_start: SystemTime::now(),
            t_end: None,
            report_dir: None,
        }
    }
}

impl ExecutionEnvBuilder {
    /// initialize ExecutionEnvBuilder
    pub fn new() -> Self {
        ExecutionEnvBuilder { ..Default::default() }
    }

    /// dry run
    pub fn dry(mut self, dry: bool) -> Self {
        self.dry = dry;
        self
    }

    /// verbose output
    pub fn verbose(mut self, verbose: u32) -> Self {
        self.verbose = verbose;
        self
    }

    /// generate report
    pub fn report(mut self, report: bool) -> Self {
        self.report = report;
        self
    }

    /// set report directory
    pub fn report_dir(mut self, report_dir: Option<PathBuf>) -> Self {
        self.report_dir = report_dir;
        self
    }

    /// progress bar size
    pub fn bar(mut self, n: u64) -> Result<Self, CollectError> {
        self.bar = Some(new_bar(n)?);
        Ok(self)
    }

    /// cli command
    pub fn cli_command(mut self, cli_command: Vec<String>) -> Self {
        self.cli_command = Some(cli_command);
        self
    }

    /// args input
    pub fn args(mut self, args: String) -> Self {
        self.args = Some(args);
        self
    }

    /// build final output
    pub fn build(self) -> ExecutionEnv {
        ExecutionEnv {
            dry: self.dry,
            verbose: self.verbose,
            report: self.report,
            bar: self.bar,
            cli_command: self.cli_command,
            args: self.args,
            t_start_parse: self.t_start_parse,
            t_start: self.t_start,
            t_end: self.t_end,
            report_dir: self.report_dir,
        }
    }
}
