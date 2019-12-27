import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, 
                      filepath):
    """
    Extracts song and artist information from song data files and
    inserts into corresponding 'songs' and 'artists' tables.
    
        Song - song_id, title, artist_id, year, duration
        Artist - artist_id, name, location, latitude, longitude
    
    :param cur: DB cursor
    :param filepath: Path of the song file to process
    """
    # open song file
    df = pd.read_json(filepath, typ="series")

    # insert song record
    song_data = df.values[[6, 7, 1, 9, 8]]
    song_data = list(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.values[[1, 5, 4, 2, 3]]
    artist_data = list(artist_data)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracts user listening data from log data and inserts 
    into Fact Table 'songplays' and Dimension Tables 'users',
    'songs', 'artists' and 'time'.
    
    :param cur: DB cursor
    :param filepath: Path of the log file to process
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = {"timestamp": t.values, 
                 "hour": t.dt.hour.values, 
                 "day": t.dt.day.values, 
                 "week": t.dt.week.values, 
                 "month": t.dt.month.values, 
                 "year": t.dt.year.values, 
                 "weekday": t.dt.weekday.values}
    
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    File processing orchestration.
    
    The 'filepath' is traversed for files to be processed 
    and found files are processed by 'func'. DB operations
    are performed using cursor 'cur'.
    
    :param cur: DB cursor
    :param conn: Connection to DB
    :param filepath: File path to traverse
    :param func: Processing function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # Establish a connection to the database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    
    # Get a cursor for the DB
    cur = conn.cursor()

    # Process the song data and log files
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # Processing finished, close the DB connection.
    conn.close()


if __name__ == "__main__":
    main()