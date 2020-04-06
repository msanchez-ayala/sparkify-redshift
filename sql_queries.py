import configparser


### CONFIG ###
config = configparser.ConfigParser()
config.read('dwh.cfg')

### DROP TABLES ###

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

### CREATE TABLES ###

staging_events_table_create= """
    CREATE TABLE IF NOT EXISTS
      staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INT,
        last_name VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration DECIMAL,
        session_id INT,
        song VARCHAR,
        status INT,
        timestamp BIGINT,
        user_agent VARCHAR,
        user_id INT
      )
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS
      staging_songs (
        num_songs INT,
        arist_id VARCHAR,
        artist_longitude NUMERIC,
        artist_latitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year INT
      )
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS
      songplays (
        songplay_id IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
      )
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS
      users (
        user_id INT UNIQUE NOT NULL PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
      )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS
      songs (
        song_id VARCHAR UNIQUE NOT NULL PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration DECIMAL
      )
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS
      artists (
        artist_id VARCHAR UNIQUE NOT NULL PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL
      )
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS
      time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour INT,
        day INT,
        week_of_year INT,
        month INT,
        year INT,
        weekday INT
      )
"""

### STAGING TABLES ###

staging_events_copy = f"""
    COPY
      staging_events
    FROM
      '{config['S3']['LOG_DATA']}'
    CREDENTIALS
      'aws_iam_role={config['DWH']['DWH_IAM_ROLE_NAME']}'
    REGION
      'us-west-2'
    FORMAT AS JSON
      {config['S3']['LOG_JSONPATH']}
"""

staging_songs_copy = f"""
    COPY
      staging_songs
    FROM
      '{config['S3']['SONG_DATA']}'
    CREDENTIALS
      'aws_iam_role={config['DWH']['DWH_IAM_ROLE_NAME']}'
    REGION
      'us-west-2'
    JSON
      'auto'
"""

### FINAL TABLES ###

songplay_table_insert = """
    INSERT INTO
      songplays (
        songplay_id,
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
      )
    VALUES
      (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
    INSERT INTO
      users (
        user_id,
        first_name,
        last_name,
        gender,
        level
      )
    VALUES
      (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

song_table_insert = """
    INSERT INTO
      songs (
        song_id,
        title,
        artist_id,
        year,
        duration
      )
    VALUES
      (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

artist_table_insert = """
    INSERT INTO
      artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
      )
    VALUES
      (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

time_table_insert = """
    INSERT INTO
      time (
        start_time,
        hour,
        day,
        week_of_year,
        month,
        year,
        weekday
      )
    VALUES
      (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

### QUERY LISTS ###

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
