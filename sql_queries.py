#OBSOLETE import configparser

#===========================================================
# DROP TABLES QUERIES
#===========================================================
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"

songplay_table_drop       = "DROP TABLE IF EXISTS factSongPlay;"
song_table_drop           = "DROP TABLE IF EXISTS dimSong;"
artist_table_drop         = "DROP TABLE IF EXISTS dimArtist;"
user_table_drop           = "DROP TABLE IF EXISTS dimUser;"
time_table_drop           = "DROP TABLE IF EXISTS dimTime;"

#===========================================================
# CREATE STAGING TABLES QUERIES
#===========================================================
staging_events_table_create = """CREATE TABLE IF NOT EXISTS staging_events (
    artist           VARCHAR,
    auth             VARCHAR,
    firstName        VARCHAR,
    gender           CHAR(1),
    itemInSession    INTEGER,
    lastName         VARCHAR,
    length           DECIMAL,
    level            VARCHAR,
    location         VARCHAR,
    method           CHAR(6),
    page             VARCHAR,
    registration     DECIMAL,
    sessionId        INTEGER,
    song             VARCHAR,
    status           INTEGER,
    ts               TIMESTAMP,
    userAgent        VARCHAR,
    userId           INTEGER
);
"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id        VARCHAR(18),  
    artist_latitude  DECIMAL(9,6),
    artist_location  VARCHAR,
    artist_longitude DECIMAL(9,6),
    artist_name      VARCHAR,
    duration         DECIMAL,
    num_songs        INTEGER,
    song_id          VARCHAR(18) NOT NULL,
    title            VARCHAR,
    year             INTEGER 
);
"""


#===========================================================
# CREATE STAR TABLES QUERIES
#===========================================================

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factSongPlay (
    songplay_id      INTEGER     IDENTITY(0,1),
    start_time       TIMESTAMP   NOT NULL SORTKEY,
    user_id          INTEGER     NOT NULL,
    level            VARCHAR,
    song_id          VARCHAR(18) NOT NULL DISTKEY,
    artist_id        VARCHAR(18) NOT NULL,
    session_id       INTEGER     NOT NULL,
    location         VARCHAR,
    user_agent       VARCHAR
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSong (
    song_id          VARCHAR(18) NOT NULL DISTKEY SORTKEY,
    title            VARCHAR     NOT NULL,
    artist_id        VARCHAR(18) NOT NULL,
    year             INTEGER     NOT NULL,
    duration         DECIMAL     NOT NULL
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtist (
    artist_id        VARCHAR(18)  NOT NULL DISTKEY SORTKEY,
    artist_name      VARCHAR      NOT NULL,
    artist_location  VARCHAR,
    artist_latitude  DECIMAL(9,6),
    artist_longitude DECIMAL(9,6)
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUser (
    user_id          INTEGER     NOT NULL DISTKEY SORTKEY,
    first_name       VARCHAR     NOT NULL,
    last_name        VARCHAR     NOT NULL,
    gender           CHAR(1)     NOT NULL,
    level            VARCHAR     NOT NULL
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime (
    start_time       TIMESTAMP   NOT NULL SORTKEY,
    hour             INTEGER,
    day              INTEGER,
    week             INTEGER,
    month            INTEGER,
    year             INTEGER,
    weekday          INTEGER
);
""")

#===========================================================
# COPY STAGING TABLES QUERIES
#
# Basic synthax is:
#  COPY table_name [ ( column_name [, ...] ) ]
#  FROM { 'filename' | PROGRAM 'command' | STDIN }
#  [ [ WITH ] ( option [, ...] ) ]
#===========================================================

staging_events_copy = ("""
COPY staging_events FROM {} 
    credentials 'aws_iam_role={}'
    region {}
    TIMEFORMAT AS 'epochmillisecs'
    JSON {};
""")

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    credentials 'aws_iam_role={}'
    region {}
    JSON 'auto';
""")


#===========================================================
# INSERT QUERY LISTS - Avoid Duplicates
#===========================================================
songplay_table_insert = ("""
INSERT INTO factSongplay (start_time, user_id, level, song_id, artist_id,
                          session_id,  location, user_agent)
    SELECT DISTINCT
        staging_events.ts        AS start_time,
        staging_events.userId    AS user_id,
        staging_events.level     AS level,
        staging_songs.song_id    AS song_id,
        staging_songs.artist_id  As artist_id,
        staging_events.sessionId AS session_id,
        staging_events.location  AS location,
        staging_events.userAgent AS user_agent
    FROM
        staging_events
    JOIN staging_songs
        ON staging_events.song   = staging_songs.title AND 
           staging_events.artist = staging_songs.artist_name        
    WHERE
        staging_events.page   = 'NextSong'
    ;
""")

user_table_insert = ("""
INSERT INTO dimUser (user_id, first_name, last_name, gender, level)
    SELECT
        userid    AS user_id,
        firstname AS first_name,
        lastname  AS last_name,
        gender    AS gender,
        level     AS level
    FROM
        (SELECT
            userid,
            firstname, 
            lastname, 
            gender, 
            level, 
            ROW_NUMBER() OVER (PARTITION BY userid) AS userid_ranked 
        FROM 
            staging_events)
    WHERE userid_ranked=1 AND userid IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO dimSong (song_id, title, artist_id, year, duration)
        SELECT song_id, title, artist_id, year, duration 
        FROM
            (SELECT
                song_id, 
                title, 
                artist_id, 
                year, 
                duration, 
                ROW_NUMBER() OVER (PARTITION BY song_id) AS song_id_ranked 
            FROM staging_songs)
        WHERE song_id_ranked = 1;
""")

artist_table_insert = ("""
    INSERT INTO dimArtist (artist_id, artist_name, artist_location, 
                            artist_latitude, artist_longitude)
        SELECT artist_id, artist_name, artist_location, 
                artist_latitude, artist_longitude
        FROM 
            staging_songs;
""")

time_table_insert = ("""
    INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
        SELECT
            ts                         AS start_time,
            EXTRACT(hour      FROM ts) AS hour,
            EXTRACT(day       FROM ts) AS day,
            EXTRACT(week      FROM ts) AS week,
            EXTRACT(month     FROM ts) AS month,
            EXTRACT(year      FROM ts) AS year,
            EXTRACT(dayofweek FROM ts) AS weekday
    FROM
        staging_events
    WHERE ts IS NOT NULL;    
""")


#===========================================================
# DROP QUERY LISTS
#===========================================================
drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]


#===========================================================
# CREATE QUERY LISTS
#===========================================================
create_staging_table_queries = [staging_events_table_create,
                                staging_songs_table_create]

create_star_table_queries = [songplay_table_create, 
                             user_table_create, 
                             song_table_create, 
                             artist_table_create, 
                             time_table_create]


#===========================================================
# COPYE QUERY LISTS
#===========================================================
copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

#===========================================================
# INSERT QUERY LISTS
#===========================================================
insert_table_queries = [artist_table_insert,
                        song_table_insert,
                        time_table_insert,
                        user_table_insert,
                        songplay_table_insert]

