# pyspark-utils
PySpark reusable scripts for ETL and ML

## Introduction


## Installation
Clone this package and run
```bash
cd path/to/cloned/dir
python setup.py
 or
pip install .
```

## Features
1. ETL
    - Extract
        - MongoDB (JSON format)
    - Transform
        - Custom User Defined Functions
    - Load
        - Normalize JSON to Delta / Parquet formats
2. ML


## Usage
```python
from pyspark_utils.etl import engine
from pyspark_utils.etl.transform import functions as cF
```