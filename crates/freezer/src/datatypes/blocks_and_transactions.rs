
    // async fn collect_chunk_with_extras(
    //     &self,
    //     block_chunk: &BlockChunk,
    //     extras: &HashSet<Datatype>,
    //     opts: &FreezeOpts,
    // ) -> HashMap<Datatype, DataFrame> {
    //     if extras.is_empty() {
    //         let df = self.collect_chunk(block_chunk, opts).await;
    //         [(Datatype::Blocks, df)].iter().cloned().collect()
    //     } else if (extras.len() == 1) & extras.contains(&Datatype::Transactions) {
    //         let numbers = chunks::get_chunk_block_numbers(block_chunk);
    //         let blocks = fetch_blocks(numbers, &opts.provider, &opts.max_concurrent_blocks)
    //             .await?
    //         let blocks = blocks.into_iter().flatten().collect();
    //         let blocks = blocks_to_df(blocks, &opts.schemas[&Datatype::Blocks])?
    //         [(Datatype::Blocks, blocks), (Datatype::Transactions, transactions)].iter().cloned().collect()
    //     } else {
    //         panic!("invalid extras")
    //     }
    // }
