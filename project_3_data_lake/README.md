# Data Lakes Project
An ETL pipeline to facilitate the transformation of Amazon S3 located Sparkify song and log data.

Data is processed using Apache Spark and stored in a S3 bucket as dimensional tables in Parquet format.

## File Structure
* /data - local copy of the data
* etl.py - script for executing the ETL pipeline
* dl.cfg - Config file containing AWS details.
* README.md - this file.

## Prerequisites
   - Python
   - AWS account with an appropriate IAM role that can access Amazon S3 buckets.

## Database Schema
Schema for Song Play Analysis

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

## Data?

`s3://udacity-dend/log_data` - Song Play Event Data (JSON)
`s3://udacity-dend/song_data` - Song Details


## Running the ETL Pipeline

1. Update the *dl.cfg* with your relevant token information for your IAM Role.

```
[AWS]
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

2. Run ETL script to process and insert data S3
    `$ python etl.py`