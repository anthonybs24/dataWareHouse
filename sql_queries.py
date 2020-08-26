import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                staging_event_id BIGINT IDENTITY(0,1) PRIMARY KEY,
                                artist VARCHAR(200),
                                auth VARCHAR(10),
                                firstName VARCHAR(100),
                                gender VARCHAR(1),
                                itemInSession INT,
                                lastName VARCHAR(100),
                                length NUMERIC,
                                level VARCHAR(4),
                                location VARCHAR(200),
                                method VARCHAR(5),
                                page VARCHAR(20),
                                registration NUMERIC,
                                sessionId INT,
                                song VARCHAR(200),
                                status INT,
                                ts VARCHAR(20),
                                userAgent VARCHAR(MAX),
                                userId INT)
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                staging_song_id BIGINT IDENTITY(0,1) PRIMARY KEY,
                                num_songs INT,
                                artist_id VARCHAR(30),
                                artist_latitude NUMERIC,
                                artist_longitude NUMERIC,
                                artist_location VARCHAR(500),
                                artist_name VARCHAR(200),
                                song_id VARCHAR(30),
                                title VARCHAR(500),
                                duration NUMERIC,
                                year INT)
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id BIGINT IDENTITY(0,1),
                            start_time TIMESTAMP NOT NULL SORTKEY,
                            user_id INT,
                            level VARCHAR(4),
                            song_id VARCHAR(20) NOT NULL,
                            artist_id VARCHAR(20) NOT NULL DISTKEY,
                            session_id INT,
                            location VARCHAR(200),
                            user_agent VARCHAR(MAX))
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id INT NOT NULL,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        gender VARCHAR(1),
                        level VARCHAR(4))
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR(20) NOT NULL,
                        title VARCHAR(200),
                        artist_id VARCHAR(20),
                        year INT SORTKEY,
                        duration NUMERIC)
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id VARCHAR(20) NOT NULL DISTKEY SORTKEY,
                            name VARCHAR(200),
                            location VARCHAR(200),
                            latitude NUMERIC,
                            longitude NUMERIC)
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP NOT NULL SORTKEY,
                        hour INT,
                        day INT,
                        week INT,
                        month VARCHAR(8),
                        year INT,
                        weekday VARCHAR(10))
                    """)

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        FORMAT AS JSON {}
                        COMPUPDATE OFF REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_events FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        FORMAT AS JSON 'auto'
                        COMPUPDATE OFF REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent)
                            VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s)
                        """)

user_table_insert = ("""INSERT INTO users (
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) 
                            DO UPDATE SET 
                                first_name = EXCLUDED.first_name, 
                                last_name = EXCLUDED.last_name, 
                                gender = EXCLUDED.gender, 
                                level = EXCLUDED.level
                    """)

song_table_insert = ("""INSERT INTO songs (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) 
                            DO UPDATE SET
                                title = EXCLUDED.title,
                                artist_id = EXCLUDED.artist_id,
                                year = EXCLUDED.year,
                                duration = EXCLUDED.duration
                    """)

artist_table_insert = ("""INSERT INTO artists (
                            artist_id,
                            name,
                            location,
                            latitude,
                            longitude)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (artist_id)
                                DO UPDATE SET
                                    name = EXCLUDED.name,
                                    location = EXCLUDED.location,
                                    latitude = EXCLUDED.latitude,
                                    longitude = EXCLUDED.longitude
                        """)

time_table_insert = ("""INSERT INTO time (
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                        VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (start_time)
                            DO NOTHING
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
