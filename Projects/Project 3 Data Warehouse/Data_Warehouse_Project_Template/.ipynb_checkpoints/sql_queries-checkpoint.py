import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
ARN = config.get('IAM_ROLE', 'ARN')


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
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itenInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        reistration FLOAT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userID INT        
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY sortkey, 
        start_time TIMESTAMP REFERENCES time(start_time), 
        user_id INT REFERENCES users(user_id) distkey,
        level VARCHAR, 
        song_id VARCHAR REFERENCES songs(song_id), 
        artist_id VARCHAR REFERENCES artists(artist_id),
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY distkey sortkey, 
        first_name VARCHAR, 
        last_name VARCHAR,
        gender VARCHAR, 
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY sortkey, 
        title VARCHAR, 
        artist_id VARCHAR REFERENCES artists(artist_id),
        year INT, 
        duration NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY sortkey, 
        name VARCHAR, 
        location VARCHAR,
        latitude NUMERIC, 
        longitude NUMERIC
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY sortkey, 
        hour INT, 
        day INT,
        week INT, 
        month INT, 
        year INT,
        weekday INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
        credentials 'aws_iam_role={}'
        format as json {}
        region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
        credentials 'aws_iam_role={}'
        format as json 'auto'
        region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, 
                            user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time, se.userID, se.level, 
                    ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events AS se JOIN staging_songs AS ss ON (se.artist = ss.artist_name AND se.song = ss.title)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userID, firstName, lastName, gender, level
    FROM staging_events
    WHERE userID IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time, EXTRACT(hour from start_time), 
                    EXTRACT(day from start_time), EXTRACT(week from start_time), EXTRACT(month from start_time), 
                    EXTRACT(year from start_time), EXTRACT(weekday from start_time)
    FROM staging_events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
