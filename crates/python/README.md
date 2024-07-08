
# ❄️🧊 cryo 🧊❄️ for python

see full [README](https://github.com/paradigmxyz/cryo?tab=readme-ov-file#installing-cryo_python-from-pypi) for details

The `cryo` python package provides the following functions that each take similar arguments:
`cryo.collect(**query)`: extract data over rpc and return as dataframe
`cryo.freeze(**query)`: extract data over rpc and save to disk
`cryo.is_data_saved(**query)`: return whether given data query has been saved to disk
`cryo.scan(**query)`: return `LazyFrame` scan of extracted data from disk
`cryo.get(**query)`: return `LazyFrame` of data from memory, disk, or rpc

The `query` uses the same parameters as the `cryo` cli.


## CRYO_ROOT
- `CRYO_ROOT` is a special directory
- [question] should this be the default or should it be an option like `--core`
- if `CRYO_ROOT` is not set, then the default value of `--output-dir` is `$PWD`
- if `CRYO_ROOT` is set, then the default value of `--output-dir` is
    - `$CRYO_ROOT/{NETWORK}/{DATATYPE}` if no special options are used
    - `$CRYO_ROOT/{NETWORK}/{DATATYPE}_{LABEL}` if label is specified
    - `$CRYO_ROOT/{NETWORK}/{DATATYPE}_{HASH(OPTIONS)}` if Content or Output options are used


## CRYO_ROOT layout
- `$CRYO_ROOT/data/{NETWORK}/{DATATYPE[_LABEL]}`: data stored directly in cryo root
- `$CRYO_ROOT/data_links/{NETWORK}/{DATATYPE[_LABEL]}/{timestamp}.link`: symlinks to data stored elsewhere on computer
- `$CRYO_ROOT/metadata`



changes to make
- change --network-name to just --network
    - use mesc to convert into chain id
    - if --rpc and --network specified, make sure they match
- add `--root` arg to use CRYO_ROOT as output dir
    - for now, refuse if any Content or Output options used, need to hash those args
- add is_data_saved()



