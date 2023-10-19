// using --remember saves the current command as the "default" for the current directory
// - this default in invoked whenever cryo is run without specifying datatypes
// - only one default command is remembered for each directory
// - remembered commands are only activated when datatypes are omitted
// - can add `--dry` or any other additional arguments to override remembered arguments

use crate::args::Args;
use cryo_freeze::ParseError;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

const REMEMBER_FILENAME: &str = "remembered_command.json";

#[derive(Serialize, Deserialize)]
pub(crate) struct RememberedCommand {
    pub(crate) cryo_version: String,
    pub(crate) command: Vec<String>,
    pub(crate) args: Args,
}

pub(crate) fn save_remembered_command(cryo_dir: PathBuf, args: &Args) -> Result<(), ParseError> {
    let cryo_version = cryo_freeze::CRYO_VERSION.to_string();
    let args = Args { remember: false, ..args.clone() };
    let command = std::env::args().filter(|w| w != "--remember").collect::<Vec<_>>();

    let remembered = RememberedCommand { cryo_version, command, args };

    let json = serde_json::to_string(&remembered).map_err(|_| {
        ParseError::ParseError("could not serialize remembered command".to_string())
    })?;
    let path = get_remembered_command_path(cryo_dir)?;
    let mut file = File::create(path)
        .map_err(|_| ParseError::ParseError("could not create remembered file".to_string()))?;
    file.write_all(json.as_bytes())
        .map_err(|_| ParseError::ParseError("could not write remembered command".to_string()))?;
    Ok(())
}

pub(crate) fn load_remembered_command(cryo_dir: PathBuf) -> Result<RememberedCommand, ParseError> {
    let path = get_remembered_command_path(cryo_dir)?;
    let mut contents = String::new();
    let mut file = File::open(path)
        .map_err(|_| ParseError::ParseError("could not open remembered file".to_string()))?;
    file.read_to_string(&mut contents)
        .map_err(|_| ParseError::ParseError("could not read rememebered file".to_string()))?;
    let remembered: RememberedCommand = serde_json::from_str(&contents)
        .map_err(|_| ParseError::ParseError("could not deserialize remembered file".to_string()))?;
    Ok(remembered)
}

pub(crate) fn get_remembered_command_path(cryo_dir: PathBuf) -> Result<PathBuf, ParseError> {
    Ok(cryo_dir.join(REMEMBER_FILENAME))
}
