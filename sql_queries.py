import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging tables :
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR (1),
    itemInSession   INT,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        TEXT,
    method          VARCHAR,
    page            VARCHAR,
    registration    VARCHAR,
    sessionId       INT,
    song            VARCHAR,
    status          INT,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs       INT,
    artist_id       VARCHAR,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name     VARCHAR,
    song_id         VARCHAR,
    title           VARCHAR,
    duration        DECIMAL,
    year            INT
);
""")

# Analytical tables
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INT IDENTITY(0,1)   NOT NULL,
    start_time      TIMESTAMP           NOT NULL, 
    user_id         INT                 NOT NULL, 
    level           VARCHAR, 
    song_id         VARCHAR,
    artist_id       VARCHAR, 
    session_id      INT, 
    location        VARCHAR, 
    user_agent      VARCHAR
) COMPOUND SORTKEY (songplay_id, start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id         INT, 
    first_name      VARCHAR, 
    last_name       VARCHAR, 
    gender          VARCHAR, 
    level           VARCHAR
) DISTSTYLE all;

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
    song_id         VARCHAR, 
    title           VARCHAR, 
    artist_id       VARCHAR, 
    year            INT, 
    duration        DECIMAL(9)
) SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR             SORTKEY, 
    name            VARCHAR, 
    location        VARCHAR, 
    latitude        VARCHAR, 
    longitude       VARCHAR
) DISTSTYLE all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      TIMESTAMP, 
    hour            SMALLINT, 
    day             SMALLINT, 
    week            SMALLINT, 
    month           SMALLINT, 
    year            SMALLINT, 
    weekday         SMALLINT
) SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
REGION {}
FORMAT as json {};

""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('DWH', 'REGION'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
REGION {}
FORMAT as JSON 'auto';

""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('DWH', 'REGION'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id,
    artist_id, session_id, location, user_agent)

    SELECT DISTINCT
    timestamp 'epoch' + ts * interval '1 second' AS start_time,
    se.userId as user_id,
    se.level as level,
    ss.song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location, 
    se.userAgent as user_agent

    FROM staging_events AS se
    JOIN staging_songs AS ss ON  ss.title = se.song AND ss.artist_name = se.artist
    WHERE se.page = 'NextSong';

""")

user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level)

    SELECT DISTINCT
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level

    FROM staging_events;

""")

song_table_insert = ("""
INSERT INTO song (
    song_id, title, artist_id, year, duration)

    SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration

    FROM staging_songs;

""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, name, location, latitude, longitude)

    SELECT DISTINCT
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude

    FROM staging_songs;

""")

time_table_insert = ("""
INSERT INTO time (
    start_time, hour, day, week, month, year, weekday)

    SELECT DISTINCT
    start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time  ) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(weekday from start_time) as weekday

    FROM songplays;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
