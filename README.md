# Data Modeling with Prostgres: Sparkify

## Purpose

The Sparkify data analytics team would like to be able to make use of listening data generated by the company's new streaming application. This project processes user activity stored as JSON logs and inserts it into a Postgres database to enable improved querying and analytics.

## Files

- **data/song_data** - Example song data.
- **data/log_data** - Example user listening data.
- **create_tables.py** - Creates the tables from create_table_queries specified in sql_queries. Note: this drops existing tables prior to creation.
- **etl.ipynb** - An interactive exploration of the song and log data and its insertion into Postgres.
- **etl.py** - Main processing script to process files into Postgres DB.
- **README.md** - This file.
- **sql_queries** - SQL statements used in processing.
- **test.ipynb** - An interactive set of tests to evaluate entries in the Postgres DB.


## Usage

### Processing files

1. Place song data files in *data/song_data*

2. Place log data files in *data/log_data*

3. Run the following:

`python3 etl.py`

