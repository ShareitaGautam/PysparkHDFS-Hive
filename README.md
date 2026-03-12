# PySpark HDFS Hive Example

This project demonstrates basic PySpark operations using data stored in HDFS and Hive.

The script reads data from HDFS, processes it using RDD and DataFrame APIs, writes data to Parquet format, and creates Hive tables including partitioned tables.

## Technologies Used

- Apache Spark (PySpark)
- Hadoop HDFS
- Apache Hive
- Python
- Docker Environment

## Features

- Read text file from HDFS using RDD
- Create Spark DataFrame from CSV
- Write DataFrame to Parquet format
- Read Parquet files from HDFS
- Create Hive tables from DataFrame
- Create partitioned Hive tables
- Run SQL queries on Hive tables using Spark SQL

## Input Files

The following files are stored in HDFS:
/data/spark/test/users.txt
/data/spark/test/zipcodes1.csv
