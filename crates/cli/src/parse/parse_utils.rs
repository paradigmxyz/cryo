use cryo_freeze::ParseError;
use std::collections::HashMap;

pub(crate) fn hex_string_to_binary(hex_string: &String) -> Result<Vec<u8>, ParseError> {
    let hex_string = hex_string.strip_prefix("0x").unwrap_or(hex_string);
    hex::decode(hex_string)
        .map_err(|_| ParseError::ParseError("could not parse data as hex".to_string()))
}

pub(crate) fn hex_strings_to_binary(hex_strings: &[String]) -> Result<Vec<Vec<u8>>, ParseError> {
    hex_strings
        .iter()
        .map(|x| {
            hex::decode(x.strip_prefix("0x").unwrap_or(x))
                .map_err(|_| ParseError::ParseError("could not parse data as hex".to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub(crate) enum BinaryInputList {
    Explicit,
    ParquetColumn(String, String),
}

use std::path::Path;

impl BinaryInputList {
    /// convert to label
    pub(crate) fn to_label(&self) -> Option<String> {
        match self {
            BinaryInputList::Explicit => None,
            BinaryInputList::ParquetColumn(path, _) => Path::new(&path)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(|stem_str| stem_str.split("__").last().unwrap_or(stem_str))
                .map(|s| s.to_string()),
        }
    }
}

type ParsedBinaryArg = HashMap<BinaryInputList, Vec<Vec<u8>>>;

/// parse binary argument list
/// each argument can be a hex string or a parquet column reference
/// each parquet column is loaded into its own list, hex strings loaded into another
pub(crate) fn parse_binary_arg(
    inputs: &[String],
    default_column: &str,
) -> Result<ParsedBinaryArg, ParseError> {
    let mut parsed = HashMap::new();

    // separate into files vs explicit
    let (files, hex_strings): (Vec<&String>, Vec<&String>) =
        inputs.iter().partition(|tx| std::path::Path::new(tx).exists());

    // files columns
    for path in files {
        let reference = parse_file_column_reference(path, default_column)?;
        let values = cryo_freeze::read_binary_column(&reference.path, &reference.column)
            .map_err(|_e| ParseError::ParseError("could not read input".to_string()))?;
        let key = BinaryInputList::ParquetColumn(reference.path, reference.column);
        parsed.insert(key, values);
    }

    // explicit binary strings
    if !hex_strings.is_empty() {
        let hex_strings: Vec<String> = hex_strings.into_iter().cloned().collect();
        let binary_vec = hex_strings_to_binary(&hex_strings)?;
        parsed.insert(BinaryInputList::Explicit, binary_vec);
    };

    Ok(parsed)
}

struct FileColumnReference {
    path: String,
    column: String,
}

fn parse_file_column_reference(
    path: &str,
    default_column: &str,
) -> Result<FileColumnReference, ParseError> {
    let (path, column) = if path.contains(':') {
        let pieces: Vec<&str> = path.split(':').collect();
        if pieces.len() == 2 {
            (pieces[0], pieces[1])
        } else {
            return Err(ParseError::ParseError("could not parse path column".to_string()))
        }
    } else {
        (path, default_column)
    };

    let parsed = FileColumnReference { path: path.to_string(), column: column.to_string() };

    Ok(parsed)
}
