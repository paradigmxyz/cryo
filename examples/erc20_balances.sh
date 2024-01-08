#!/usr/bin/env bash

# get ERC20 balance of a wallet over some block range

# ERC20 = USDC
export CONTRACT=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48

# Wallet = Maker PSM
export WALLET=0x0a59649758aa4d66e25f08dd01271e891fe52199

# selector for balanceOf(address)
export FUNCTION=0x70a08231

# block range = 10 blocks between 16,000,000 and 17,000,000
export BLOCKS="16M:17M/10"

cryo eth_calls \
    --label erc20_balances \
    --blocks $BLOCKS \
    --contract $CONTRACT \
    --function $FUNCTION \
    --inputs $(printf '%064s' ${WALLET:2})

