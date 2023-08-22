use std::collections::{HashMap, HashSet};
use ethers::abi::Event;
use ethers_core::abi::{ethabi, HumanReadableParser, LogParam, RawLog, Token};
use ethabi::param_type::ParamType;
use ethers::prelude::{Address, Log};

#[tokio::main]
async fn main() {
    // let event: Event = serde_json::from_str(EVENT_S).unwrap();
    // println!("{:?}", event.inputs);
    // println!("{:#?}", event);


    let e = HumanReadableParser::parse_event("event NewMint(address indexed msgSender, uint256 indexed mintQuantity)").unwrap();

    let raw_log = r#"{
        "address": "0x0000000000000000000000000000000000000000",
        "topics": [
            "0x52277f0b4a9b555c5aa96900a13546f972bda413737ec164aac947c87eec6024",
            "0x00000000000000000000000062a73d9116eda78a78f4cf81602bdc926fb4c0dd",
            "0x0000000000000000000000000000000000000000000000000000000000000003"
        ],
        "data": "0x"
    }"#;

    let log = serde_json::from_str::<Log>(raw_log).unwrap();

    let m = parse_log_from_event(e, vec![log]);
    println!("{:#?}", m);
}

// this function assumes all logs are of the same type and skips them if they aren't
fn parse_log_from_event(event: ethers::abi::Event, logs: Vec<Log>) -> HashMap<String, Vec<Token>> {
    let mut map: HashMap<String, Vec<Token>> = HashMap::new();

    let known_keys = event.inputs.clone().into_iter().map(|i| i.name).collect::<HashSet<String>>();

    for log in logs {
        let l = event.parse_log(RawLog::from(log)).unwrap();
        for param in l.params {
            if known_keys.contains(param.name.as_str()) {
                let tokens = map.entry(param.name).or_insert(Vec::new());
                tokens.push(param.value);
            }
        }
    }
    map
}


// const EVENT_S: &str = r#"{"name":"bar","inputs":[{"name":"a","type":"uint256"},{"name":"b","type":"bool"}],"anonymous":false,"type":"event"}"#;
const EVENT_S: &str = r#"{"anonymous": false,"inputs": [{"indexed": true,"name": "msgSender","type": "address"},{"indexed": true,"name": "mintQuantity","type": "uint256"}],"name": "NewMint","type": "event"}"#;