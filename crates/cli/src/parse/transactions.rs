use cryo_freeze::{Chunk, ParseError, TransactionChunk};
use polars::prelude::*;

pub(crate) fn parse_transactions(txs: &[String]) -> Result<Vec<Chunk>, ParseError> {
    let (files, hashes): (Vec<&String>, Vec<&String>) =
        txs.iter().partition(|tx| std::path::Path::new(tx).exists());

    let file_chunks = if files.len() > 0 {
        let file_chunks = Vec::new();
        for path in files {
            let column = if path.contains(":") {
                path.split(":")
                    .last()
                    .ok_or(ParseError::ParseError("could not parse txs path column".to_string()))?
            } else {
                "transaction_hash"
            };
            let mut path = std::fs::File::open(path)
                .map_err(|e| ParseError::ParseError("could not open file path".to_string()))?;
            let df = ParquetReader::new(path).with_columns(Some(vec![column.to_string()])).finish();
            let chunk = TransactionChunk::Values(df[column]);
            file_chunks.push(Chunk::Transaction(chunk));
        }
        file_chunks
    } else {
        Vec::new()
    };

    let hash_chunks = if hashes.len() > 0 {
        let values: Result<Vec<Vec<u8>>, _> = hashes.iter().map(hex::decode).collect();
        let values =
            values.map_err(|_e| ParseError::ParseError("could not parse txs".to_string()))?;
        let chunk = Chunk::Transaction(TransactionChunk::Values(values));
        vec![chunk]
    } else {
        Vec::new()
    };

    file_chunks.extend(hash_chunks);
    file_chunks
}
