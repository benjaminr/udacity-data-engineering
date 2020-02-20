# Data Warehouse Project
## Introduction

The project facilitates the transformation of Amazon S3 located Sparkify app data into a Amazon Redshift cluster.

It provides an ETL pipeline for extracting, staging, and transforming data into a set of dimensional tables, readying the data for the analytics team to continue finding insights.

## File Structure
* create_tables.py - script for preparing and creating the database tables.
* etl.py - script for executing the ETL pipeline
* sql_queries.py - file to logically seperate the SQL queries from the business logic.
* dwh.cfg - Data Warehouse Config file containing AWS S3 and Redshift details.
* README.md - this file.

## Prerequisites
You will need Python.
You will need a Redshift Cluster with an appropriate IAM role that can access Amazon S3 buckets.

## Data?
`s3://udacity-dend/log_data` - Song Play Event Data (JSON)
`s3://udacity-dend/song_data` - Song Details

## Database Schema

### Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong
  * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  
### Dimension Tables
* users - users in the app
  * user_id, first_name, last_name, gender, level
* songs - songs in music database
  * song_id, title, artist_id, year, duration
* artists - artists in music database
  * artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
  * start_time, hour, day, week, month, year, weekday

## Running the ETL Pipeline

1. Update the *dwh.cfg* with your relevant Redshift cluster information and ARN for your IAM Role.

```
[CLUSTER]
HOST=
DB_NAME=dev
DB_USER=awsuser
DB_PASSWORD=
DB_PORT=5439

[IAM_ROLE]
ARN=

[S3]
LOG_DATA=s3://udacity-dend/log_data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song_data
```

2. Run create_tables script to prepare database tables
    `$ python create_tables.py`

3. Run ETL script to copy and insert data into Redshift
    `$ python etl.py`
