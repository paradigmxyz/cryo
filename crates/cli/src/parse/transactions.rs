use cryo_freeze::{Chunk, ParseError, TransactionChunk};

pub(crate) fn parse_transactions(
    txs: &[String],
) -> Result<Vec<(Chunk, Option<String>)>, ParseError> {
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
            let tx_hashes = cryo_freeze::read_binary_column(path, column)
                .map_err(|_e| ParseError::ParseError("could not read input".to_string()))?;
            let chunk = TransactionChunk::Values(tx_hashes);
            let chunk_label = path
                .split("__")
                .last()
                .and_then(|s| s.strip_suffix(".parquet").map(|s| s.to_string()));
            file_chunks.push((Chunk::Transaction(chunk), chunk_label));
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
        vec![(chunk, None)]
    } else {
        Vec::new()
    };

    file_chunks.extend(hash_chunks);
    Ok(file_chunks)
}
