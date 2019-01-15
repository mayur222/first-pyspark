# first-pyspark
PySpark script for basic data quality check on CSV file

This script will automatically create stats file.
Then will show unique column if there is any.
If not it'll show combination of column which is unique.
Then it'll check for duplicates and print those rows with number of duplicates present.

## Requirements
pyspark 2.4.0
java 1.8

## Usage:
```
python DQ.py Test.csv
```
