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
        auth VARCHAR(20),
        first_name VARCHAR(30),
        gender VARCHAR(1),
        item_in_session INT,
        last_name VARCHAR(30),
        length FLOAT,
        level VARCHAR(4),
        location VARCHAR,
        method VARCHAR(3),
        page VARCHAR(10),
        registration DECIMAL,
        session_id INT,
        song VARCHAR,
        status SMALLINT,
        timestamp BIGINT,
        user_agent VARCHAR,
        user_id INT
      )
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS
      staging_songs (
        num_songs SMALLINT,
        arist_id VARCHAR(25),
        artist_longitude FLOAT,
        artist_latitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR(25),
        title VARCHAR,
        duration FLOAT,
        year SMALLINT
      )
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS
      songplays (
        songplay_id IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INT REFERENCES users,
        level VARCHAR(4),
        song_id VARCHAR(25) REFERENCES songs,
        artist_id VARCHAR(25) REFERENCES artists,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
      )
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS
      users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(30),
        last_name VARCHAR(30) SORTKEY,
        gender VARCHAR(1),
        level VARCHAR(4)
      )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS
      songs (
        song_id VARCHAR(25) PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR(25) NOT NULL DISTKEY REFERENCES artists,
        year SMALLINT,
        duration FLOAT
      )
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS
      artists (
        artist_id VARCHAR(25) PRIMARY KEY DISTKEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
      )
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS
      time (
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT,
        day SMALLINT,
        week_of_year SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
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
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
      )
    SELECT
      TIMESTAMP 'epoch' + e.ts * INTERVAL '1 second' as start_time,
      e.user_id,
      e.level,
      s.song_id,
      s.artist_id,
      e.session_id,
      e.location,
      e.user_agent
    FROM
      staging_events e
    JOIN
      staging_songs s
    ON
      e.artist = s.artist_name AND
      e.length = s.duration AND
      e.song = s.title
    WHERE
      e.page = 'NextSong'
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
