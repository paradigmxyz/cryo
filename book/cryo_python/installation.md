
## Installation

There are two main options for installing `cryo`.

Installing from pip is faster and does not require rust to be installed.

Installing from source allows using the latest unreleased version of cryo.

#### Option 1: Install from pip

```
pip install cryo
```

#### Option 2: Install from source

```
pip install maturin
git clone https://github.com/paradigmxyz/cryo
cd cryo/crates/python
maturin build --release
pip install --force-reinstall <OUTPUT_OF_MATURIN_BUILD>.whl
```

#### Other notes

If you would like `cryo` to output results using pandas instead of polars, also install pandas: `pip install pandas`

