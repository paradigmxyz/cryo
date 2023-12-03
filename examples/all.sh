#!/usr/bin/env bash

# This is an example of collecting most of the cryo datasets

#
# # parameters
#

# use installed cryo installation
EXECUTABLE=cryo

# use local cargo repo
# EXECUTABLE="cargo run --"

# use some other cargo repo
# EXECUTABLE="cargo run --manifest-path MANIFEST_PATH --"

BLOCKS="18M:+100"  # collect blocks 18,000,000 to 18,000,100
OUTPUT_DIR="data"  # output directory


#
# # datasets
#

$EXECUTABLE address_appearances -b $BLOCKS -o $OUTPUT_DIR/address_appearances

$EXECUTABLE balance_diffs -b $BLOCKS -o $OUTPUT_DIR/balance_diffs

$EXECUTABLE balances -b $BLOCKS -o $OUTPUT_DIR/balances --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE blocks -b $BLOCKS -o $OUTPUT_DIR/blocks

$EXECUTABLE code_diffs -b $BLOCKS -o $OUTPUT_DIR/code_diffs

$EXECUTABLE codes -b $BLOCKS -o $OUTPUT_DIR/codes --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE contracts -b $BLOCKS -o $OUTPUT_DIR/contracts

$EXECUTABLE erc20_balances -b $BLOCKS -o $OUTPUT_DIR/erc20_balances --contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE erc20_metadata -b $BLOCKS -o $OUTPUT_DIR/erc20_metadata --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE erc20_supplies -b $BLOCKS -o $OUTPUT_DIR/erc20_supplies --contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE erc20_transfers -b $BLOCKS -o $OUTPUT_DIR/erc20_transfers

$EXECUTABLE erc721_metadata -b $BLOCKS -o $OUTPUT_DIR/erc721_metadata --contract 0xed5af388653567af2f388e6224dc7c4b3241c544 &

$EXECUTABLE erc721_transfers -b $BLOCKS -o $OUTPUT_DIR/erc721_transfers --contract 0xed5af388653567af2f388e6224dc7c4b3241c544 &

$EXECUTABLE eth_calls -b $BLOCKS -o $OUTPUT_DIR/eth_calls --call-data 0x18160ddd --contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE logs -b $BLOCKS -o $OUTPUT_DIR/logs

$EXECUTABLE native_transfers -b $BLOCKS -o $OUTPUT_DIR/native_transfers

$EXECUTABLE nonce_diffs -b $BLOCKS -o $OUTPUT_DIR/nonce_diffs

$EXECUTABLE nonces -b $BLOCKS -o $OUTPUT_DIR/nonces --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE state_diffs -b $BLOCKS -o $OUTPUT_DIR/state_diffs

$EXECUTABLE storage_diffs -b $BLOCKS -o $OUTPUT_DIR/storage_diffs

$EXECUTABLE storages -b $BLOCKS -o $OUTPUT_DIR/storages --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --slot 0x0000000000000000000000000000000000000000000000000000000000000000 &

$EXECUTABLE trace_calls -b $BLOCKS -o $OUTPUT_DIR/trace_calls --call-data 0x18160ddd --contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 &

$EXECUTABLE traces -b $BLOCKS -o $OUTPUT_DIR/traces

$EXECUTABLE transactions -b $BLOCKS -o $OUTPUT_DIR/transactions

$EXECUTABLE blocks transactions -b $BLOCKS -o $OUTPUT_DIR/blocks_transactions

$EXECUTABLE state_diffs -b $BLOCKS -o $OUTPUT_DIR/state_diffs
