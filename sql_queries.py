# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (start_time bigint NOT NULL, 
                                      user_id int NOT NULL, 
                                      level text NOT NULL, 
                                      song_id text, 
                                      artist_id text, 
                                      session_id text NOT NULL, 
                                      location text, 
                                      user_agent text NOT NULL, 
                                      PRIMARY KEY (start_time, user_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id text UNIQUE PRIMARY KEY, 
                                  first_name text NOT NULL, 
                                  last_name text NOT NULL, 
                                  gender text NOT NULL, 
                                  level text NOT NULL)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id text UNIQUE PRIMARY KEY, 
                                  title text NOT NULL, 
                                  artist_id text NOT NULL, 
                                  year int NOT NULL, 
                                  duration int NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id text UNIQUE PRIMARY KEY, 
                                    name text NOT NULL, 
                                    location text, 
                                    latitude decimal, 
                                    longitude decimal)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, 
                                 hour int NOT NULL, 
                                 day int NOT NULL, 
                                 week int NOT NULL, 
                                 month int NOT NULL, 
                                 year int NOT NULL, 
                                 weekday int NOT NULL)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, 
                       user_id, 
                       level, 
                       song_id, 
                       artist_id, 
                       session_id, 
                       location, 
                       user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, 
                   first_name, 
                   last_name, 
                   gender, 
                   level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE
SET first_name = excluded.first_name,
    last_name = excluded.last_name,
    gender = excluded.gender,
    level = excluded.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, 
                   title, 
                   artist_id, 
                   year, 
                   duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, 
                     name, 
                     location, 
                     latitude, 
                     longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time (start_time, 
                  hour, 
                  day, 
                  week, 
                  month, 
                  year, 
                  weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

# FIND SONGS
song_select = ("""
SELECT song_id, artists.artist_id 
FROM songs 
JOIN artists ON songs.artist_id=artists.artist_id 
WHERE title=%s
AND artists.name=%s
AND duration=ROUND(%s)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
