import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GET VARS

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", 'LOG_DATA')
LOG_JSONPATH = config.get("S3", 'LOG_JSONPATH')
SONG_DATA = config.get("S3", 'SONG_DATA')
REGION = "us-west-2"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    event_id      BIGINT IDENTITY(0,1) NOT NULL,
    artist        VARCHAR              NULL,
    auth          VARCHAR              NULL,
    firstName     VARCHAR              NULL,
    gender        VARCHAR              NULL,
    itemInSession VARCHAR              NULL,
    lastName      VARCHAR              NULL,
    length        VARCHAR              NULL,
    level         VARCHAR              NULL,
    location      VARCHAR              NULL,
    method        VARCHAR              NULL,
    page          VARCHAR              NULL,
    registration  VARCHAR              NULL,
    sessionId     VARCHAR              NOT NULL SORTKEY DISTKEY, 
    song          VARCHAR              NULL,
    status        INTEGER              NULL,
    ts            TIMESTAMP            NULL,
    userAgent     VARCHAR              NULL,
    userId        VARCHAR              NULL
   );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs        INTEGER       NULL,
    artist_id        VARCHAR       NOT NULL SORTKEY DISTKEY,
    artist_latitude  FLOAT         NULL,
    artist_longitude FLOAT         NULL,
    artist_location  VARCHAR       NULL,
    artist_name      VARCHAR       NULL,
    song_id          VARCHAR       NOT NULL,
    title            VARCHAR       NULL,
    duration         FLOAT         NULL,
    year             INTEGER       NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id INTEGER IDENTITY(0,1) SORTKEY,
    start_time  TIMESTAMP    NOT NULL,
    user_id     VARCHAR      NOT NULL DISTKEY,
    level       VARCHAR      NOT NULL,
    song_id     VARCHAR      NOT NULL,
    artist_id   VARCHAR      NOT NULL,
    session_id  VARCHAR      NOT NULL,
    location    VARCHAR      NOT NULL,
    user_agent  VARCHAR      NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id    VARCHAR     NOT NULL SORTKEY, 
    first_name VARCHAR     NOT NULL, 
    last_name  VARCHAR     NOT NULL, 
    gender     VARCHAR     NULL,
    level      VARCHAR     NOT NULL
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id   VARCHAR      SORTKEY, 
    title     VARCHAR      NOT NULL, 
    artist_id VARCHAR      NOT NULL, 
    year      INTEGER, 
    duration  DECIMAL(10)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR      SORTKEY, 
    name      VARCHAR      NOT NULL, 
    location  VARCHAR, 
    latitude  DECIMAL(10),
    longitude DECIMAL(10)
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time TIMESTAMP   SORTKEY, 
    hour       INTEGER, 
    day        INTEGER, 
    week       INTEGER, 
    month      INTEGER, 
    year       INTEGER, 
    weekday    INTEGER
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
credentials 'aws_iam_role={}'
TIMEFORMAT as 'epochmillisecs'
format as json '{}'
STATUPDATE ON
region '{}';
""").format(LOG_DATA, ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
credentials 'aws_iam_role={}'
format as json 'auto'
region '{}';
""").format(SONG_DATA, ARN, REGION)

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
SELECT 
    se.ts                 AS start_time,
    se.userId             AS user_id,
    se.level              AS level,
    ss.song_id            AS song_id,
    ss.artist_id          AS artist_id,
    se.sessionId          AS session_id,
    se.location           AS location,
    se.userAgent          AS user_agent
FROM staging_events as se
JOIN staging_songs as ss ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, 
                   first_name, 
                   last_name, 
                   gender, 
                   level)
SELECT  DISTINCT(userId)    AS user_id,
        firstName           AS first_name,
        lastName            AS last_name,
        gender,
        level
FROM staging_events
WHERE user_id IS NOT NULL
AND page  =  'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, 
                   title, 
                   artist_id, 
                   year, 
                   duration)
SELECT  DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, 
                     name, 
                     location, 
                     latitude, 
                     longitude)
SELECT  DISTINCT(artist_id) AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, 
                  hour, 
                  day, 
                  week, 
                  month, 
                  year, 
                  weekday)
SELECT  DISTINCT(start_time)                AS start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(dayofweek FROM start_time)  AS weekday
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
