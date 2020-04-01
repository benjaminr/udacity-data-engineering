import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, year, \
                                  month, dayofmonth, hour, 
                                  weekofyear, date_format, \
                                  monotonically_increasing_id


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark,
                      input_data,
                      output_data):
    """
    Processes song data and outputs parquet patitioned data to S3.

    :param spark: Spark instance
    :param input_data: input S3 location
    :param output_data: output S3 location
    """
    # get filepath to song data file
    song_data = f"{input_data}/song_data/A/A/A/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id',
                     'title',
                     'artist_id',
                     'year',
                     'duration']

    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
               .partitionBy(['year', 'artist_id']) \
               .parquet(os.path.join(output_data, 'songs.pqt'), 'overwrite')
    songs_table = songs_table.drop_duplicates(subset=['song_id'])

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name',
                       'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(
        output_data, 'artists.pqt'), 'overwrite')


def process_log_data(spark,
                     input_data,
                     output_data):
    """
    Processes log data and outputs parquet patitioned data to S3.

    :param spark: Spark instance
    :param input_data: input S3 location
    :param output_data: output S3 location
    """
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    song_plays = df[df.page == 'NextSong']

    # extract columns for users table
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.drop_duplicates(subset=['userId'])

    # write users table to parquet files
    users_table.write \
               .parquet(os.path.join(output_data, "users.pqt"), 'overwrite')

    # create datetime column from timestamp column
    get_dt = udf(lambda t: datetime.fromtimestamp(t // 1000))
    df = df.withColumn('datetime', get_dt(df.ts))

    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year')
    )
    time_table = time_table.dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write \
              .partitionBy(['year', 'month']) \
              .parquet(os.path.join(output_data, "time.pqt"), 'overwrite')

    # read in song data to use for songplays table
    song_data = f"{input_data}/song-data/A/A/A/*.json"
    song_df = spark.read \
                   .json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    df = df['datetime', 'userId', 'level', 'song',
            'artist', 'sessionId', 'location', 'userAgent']

    log_song_df = df.join(song_df, df.song == song_df.title)

    songplays_table = log_song_df.select(
        col('datetime').alias('ts'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    )

    songplays_table.select(
        monotonically_increasing_id().alias('songplay_id')).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
                   .partitionBy('year', 'month') \
                   .parquet(os.path.join(output_data, "songplays.pqt"), 'overwrite')


def main():

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://INSERT_BUCKET_NAME_HERE/"

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    print(os.environ['AWS_ACCESS_KEY_ID'])

    spark = create_spark_session()

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
