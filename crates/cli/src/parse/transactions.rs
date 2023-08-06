use cryo_freeze::{Chunk, ParseError, TransactionChunk};
use polars::prelude::*;

pub(crate) fn parse_transactions(txs: &[String]) -> Result<Vec<Chunk>, ParseError> {
    let (files, hashes): (Vec<&String>, Vec<&String>) =
        txs.iter().partition(|tx| std::path::Path::new(tx).exists());

    let mut file_chunks = if !files.is_empty() {
        let mut file_chunks = Vec::new();
        for path in files {
            let column = if path.contains(':') {
                path.split(':')
                    .last()
                    .ok_or(ParseError::ParseError("could not parse txs path column".to_string()))?
            } else {
                "transaction_hash"
            };
            let tx_hashes = read_binary_column(path, column)
                .map_err(|_e| ParseError::ParseError("could not read input".to_string()))?;
            let chunk = TransactionChunk::Values(tx_hashes);
            file_chunks.push(Chunk::Transaction(chunk));
        }
        file_chunks
    } else {
        Vec::new()
    };

    let hash_chunks = if !hashes.is_empty() {
        let values: Result<Vec<Vec<u8>>, _> = hashes.iter().map(hex::decode).collect();
        let values =
            values.map_err(|_e| ParseError::ParseError("could not parse txs".to_string()))?;
        let chunk = Chunk::Transaction(TransactionChunk::Values(values));
        vec![chunk]
    } else {
        Vec::new()
    };

    file_chunks.extend(hash_chunks);
    Ok(file_chunks)
}

fn read_binary_column(path: &str, column: &str) -> Result<Vec<Vec<u8>>, ParseError> {
    let file = std::fs::File::open(path)
        .map_err(|_e| ParseError::ParseError("could not open file path".to_string()))?;

    let df = ParquetReader::new(file)
        .with_columns(Some(vec![column.to_string()]))
        .finish()
        .map_err(|_e| ParseError::ParseError("could not read data from column".to_string()))?;

    let series = df
        .column(column)
        .map_err(|_e| ParseError::ParseError("could not get column".to_string()))?;

    let ca = series
        .binary()
        .map_err(|_e| ParseError::ParseError("could not convert to binary column".to_string()))?;

    ca.into_iter()
        .map(|value| {
            value
                .ok_or_else(|| ParseError::ParseError("transaction hash missing".to_string()))
                .map(|data| data.into())
        })
        .collect()
}
