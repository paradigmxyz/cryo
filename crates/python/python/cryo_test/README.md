
# cryo test

utility for performing comparisons between cryo environments

```bash
cryo_test is a cli tool for comparing cryo outputs across different conditions

Usage: cryo_test QUERY [OPTIONS]

This will 1. setup comparison dir, 2. collect data, and 3. compare outputs

Examples:
  cryo_test --rpc source1=https://h1.com source2=https://h2.com (compare rpc's)
  cryo_test --executable old=/path/to/cryo1 new=/path/to/cryo2 (compare executables)
  cryo_test --python ... (use python)
  cryo_test --cli-vs-python ... (compare cli vs python)

Options:
  -h, --help                                 show this help message and exit
  -e, --executable, --executables EXECUTABLE [...]
                                             executable(s) to use
  --rpc RPC [...]                            rpc endpoint(s) to use
  -d, --datatype, --datatypes DATATYPE [...]
                                             datatype(s) to collect
  -p, --python                               use python for all batches
  --cli-vs-python                            compare cli to python
  -r, --rerun                                rerun previous comparison
  --label LABEL                              name of comparison
  -s, --steps {setup,collect,compare} [...]  steps to perform {setup, collect, compare}
  --dir DIR                                  directory for storing comparison data
  -i, --interactive                          load data in interactive python session
  --scan-interactive                         scan data in interactive python session
  --debug, --pdb                             enter debug mode upon error
```
