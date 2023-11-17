# This script extracts all pools and swaps for Uniswap v2 / v3

# This script is
# - idempotent: run the script as many times as you want, the data will be fine
# - interuptable: you can interrupt the script whenever you want, the data will be fine
# - incremental: only missing data is collected. re-runing the script does not re-collect data

# uniswap v2 pools
cryo logs \
    --label uniswap_v2_pools \
    --blocks 10M: \
    --reorg-buffer 1000 \
    --subdirs datatype \
    --contract 0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f \
    --event-signature "PairCreated(address indexed token0, address indexed token1, address pair, uint)" \
    --topic0 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9

# uniswap v3 pools
cryo logs \
    --label uniswap_v3_pools \
    --blocks 12.369M: \
    --reorg-buffer 1000 \
    --subdirs datatype \
    --contract 0x1f98431c8ad98523631ae4a59f267346ea31f984 \
    --event-signature "PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)" \
    --topic0 0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118

# uniswap v2 swaps
cryo logs \
    --label uniswap_v2_swaps \
    --blocks 10M: \
    --reorg-buffer 1000 \
    --subdirs datatype \
    --event-signature "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)" \
    --topic0 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822

# uniswap v3 swaps
cryo logs \
    --label uniswap_v3_swaps \
    --blocks 12.369M: \
    --reorg-buffer 1000 \
    --subdirs datatype \
    --event-signature "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)" \
    --topic0 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67

