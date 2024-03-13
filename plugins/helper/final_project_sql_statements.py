class SqlQueries:
    staging_events_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length DOUBLE PRECISION,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    )
    """)

    staging_songs_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(512),
        artist_name VARCHAR(512),
        song_id VARCHAR,
        title VARCHAR(512),
        duration DOUBLE PRECISION,
        year INTEGER
    )
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
        songplay_id VARCHAR PRIMARY KEY, 
        start_time TIMESTAMP, 
        user_id INTEGER , 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INTEGER, 
        location VARCHAR, 
        user_agent VARCHAR
    )
    """)

    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
    )
    """)

    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INTEGER, 
    duration DOUBLE PRECISION
    )
    """)

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    artist_name VARCHAR, 
    artist_location VARCHAR, 
    artist_latitude DOUBLE PRECISION, 
    artist_longitude DOUBLE PRECISION
    )
    """)

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER
    )
    """)

    songplay_table_insert = ("""
        INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid AS user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid AS session_id, 
                events.location, 
                events.useragent AS user_agent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
              FROM staging_events
              WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT 
                distinct userid AS user_id, 
                firstname AS first_name, 
                lastname AS last_name, 
                gender, 
                level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time) as hour, extract(day from start_time) as day, extract(week from start_time) as week, 
               extract(month from start_time) as month, extract(year from start_time) as year, extract(dayofweek from start_time) as weekday
        FROM songplays
    """)
