/// get name of network if known
pub fn get_network_name(chain_id: u64) -> Option<String> {
    match chain_id {
        1 => Some("ethereum"),
        5 => Some("goerli"),
        10 => Some("optimism"),
        56 => Some("bnb"),
        69 => Some("optimism_kovan"),
        100 => Some("gnosis"),
        137 => Some("polygon"),
        420 => Some("optimism_goerli"),
        1101 => Some("polygon_zkevm"),
        1442 => Some("polygon_zkevm_testnet"),
        8453 => Some("base"),
        10200 => Some("gnosis_chidao"),
        17000 => Some("holesky"),
        42161 => Some("arbitrum"),
        42170 => Some("arbitrum_nova"),
        43114 => Some("avalanche"),
        80001 => Some("polygon_mumbai"),
        84531 => Some("base_goerli"),
        7777777 => Some("zora"),
        11155111 => Some("sepolia"),
        _ => None,
    }
    .map(|x| x.to_string())
}
