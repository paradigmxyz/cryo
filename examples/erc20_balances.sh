#!/usr/bin/env bash

# get ERC20 balances of addresses over some block range

# ERC20 = USDC, USDT
export ERC20="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 0xdac17f958d2ee523a2206206994597c13d831ec7"

# WALLET = Maker PSM, Arbitrum Brdige
export WALLETS="0x0a59649758aa4d66e25f08dd01271e891fe52199 0xcee284f754e854890e311e3280b767f80797180d"

# block range = 10 blocks between 16,000,000 and 17,000,000
export BLOCKS="16M:17M/10"

# collect data
cryo erc20_balances --blocks $BLOCKS --contract $ERC20 --address $WALLET

