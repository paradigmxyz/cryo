from __future__ import annotations


datatypes = [
    'address_appearances',
    'balance_diffs',
    'balances',
    'blocks',
    'code_diffs',
    'codes',
    'contracts',
    'erc20_balances',
    'erc20_metadata',
    'erc20_supplies',
    'erc20_transfers',
    'erc721_metadata',
    'erc721_transfers',
    'eth_calls',
    'logs',
    'native_transfers',
    'nonce_diffs',
    'nonces',
    'storage_diffs',
    'slots',
    'trace_calls',
    'traces',
    'transactions',
    # 'vm_traces',
]

multi_datatypes = [
    'blocks transactions',
    'state_diffs',
]

multi_aliases = {
    'state_diffs': [
        'balance_diffs',
        'code_diffs',
        'nonce_diffs',
        'storage_diffs',
    ],
}

datatype_parameters: dict[str | tuple[str, ...], str] = {
    ('balances',): '--address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ('codes',): '--address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ('nonces',): '--address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    (
        'erc20_balances',
    ): '--contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ('erc20_metadata',): '--address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    (
        'erc20_supplies',
    ): '--contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    (
        'erc721_metadata',
    ): '--contract 0xed5af388653567af2f388e6224dc7c4b3241c544',
    (
        'erc721_transfers',
    ): '--contract 0xed5af388653567af2f388e6224dc7c4b3241c544',
    (
        'eth_calls',
    ): '--call-data 0x18160ddd --contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    (
        'slots',
    ): '--address 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --slot 0x0000000000000000000000000000000000000000000000000000000000000000',
    (
        'trace_calls',
    ): '--call-data 0x18160ddd --contract 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
}

not_by_transaction = [
    'balances',
    'codes',
    'erc20_balances',
    'erc20_metadata',
    'erc20_supplies',
    'erc721_metadata',
    'eth_calls',
    'nonces',
    'slots',
    'trace_calls',
]

time_dimensions = {
    'blocks': '-b 18.000M:18.0001M',
    'transactions': '--txs 0xa1935e783e5e7cd320c6e9005a8a7770853d2e46837ac31de1afcfe8b4b7079d',
}

default_combos: dict[str, list[str]] = {
    'datatype': datatypes + multi_datatypes,
    'time_dimension': ['blocks', 'transactions'],
}
