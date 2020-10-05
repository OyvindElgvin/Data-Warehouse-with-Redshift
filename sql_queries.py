import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events_table (artist        varchar,
                                                 auth          varchar,
                                                 firstName     varchar,
                                                 gender        varchar,
                                                 itemInSession int,
                                                 lastName      varchar, 
                                                 length        float,
                                                 level         varchar,
                                                 location      varchar,
                                                 method        varchar,
                                                 page          varchar,
                                                 registration  varchar,
                                                 sessionId     varchar,
                                                 song          varchar,
                                                 status        varchar,
                                                 ts            bigint,
                                                 userAgent     varchar,
                                                 userId        int);""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (num_songs        int,
                                                artist_id        varchar sortkey, 
                                                artist_latitude  float, 
                                                artist_longitude float, 
                                                artist_location  varchar, 
                                                artist_name      varchar,
                                                song_id          varchar, 
                                                title            varchar, 
                                                duration         float, 
                                                year             int);""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id  int IDENTITY(0,1) NOT NULL PRIMARY KEY, 
                                                                  start_time   timestamp, 
                                                                  user_id      int NOT NULL, 
                                                                  level        varchar, 
                                                                  song_id      varchar, 
                                                                  artist_id    varchar sortkey, 
                                                                  session_id   varchar, 
                                                                  location     varchar, 
                                                                  user_agent   varchar);""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id    int NOT NULL PRIMARY KEY, 
                                                          first_name varchar, 
                                                          last_name  varchar, 
                                                          gender     varchar, 
                                                          level      varchar);""")


song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id   varchar NOT NULL PRIMARY KEY, 
                                                          title     varchar, 
                                                          artist_id varchar sortkey, 
                                                          year      int, 
                                                          duration  float);""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar NOT NULL PRIMARY KEY sortkey, 
                                                              name      varchar, 
                                                              location  varchar, 
                                                              latitude  float, 
                                                              longitude float);""")


time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp NOT NULL PRIMARY KEY, 
                                                         hour       int, 
                                                         day        int, 
                                                         week       int, 
                                                         month      int, 
                                                         year       int, 
                                                         weekday    int);""")

# STAGING TABLES


staging_events_copy = ("""COPY staging_events_table FROM {}
                          credentials 'aws_iam_role={}'
                          COMPUPDATE OFF region 'us-west-2'
                          TIMEFORMAT as 'epochmillisecs'
                          STATUPDATE ON
                          FORMAT AS JSON {};
                          """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""COPY staging_songs_table FROM {}
                         credentials 'aws_iam_role={}'
                         COMPUPDATE OFF region 'us-west-2'
                         FORMAT AS JSON 'auto';
                         """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

 


# FINAL TABLES


songplay_table_insert = ("""INSERT INTO songplays (start_time, 
                                                   user_id, 
                                                   level, 
                                                   song_id, 
                                                   artist_id, 
                                                   session_id, 
                                                   location, 
                                                   user_agent)
                                            SELECT DISTINCT DATE_ADD('ms', se.ts, '1970-01-01') AS start_time,
                                                   se.userId, 
                                                   se.level, 
                                                   so.song_id, 
                                                   so.artist_id, 
                                                   se.sessionId, 
                                                   se.location, 
                                                   se.userAgent
                                              FROM staging_events_table se
                                         LEFT JOIN staging_songs_table so
                                                ON se.song = so.title 
                                               AND se.length = so.duration
                                               AND se.artist = so.artist_name
                                             WHERE se.page = 'NextSong'
                                               AND se.userId IS NOT NULL;""")


user_table_insert = ("""INSERT INTO users (user_id, 
                                           first_name, 
                                           last_name, 
                                           gender, 
                                           level) 
                                    SELECT DISTINCT userId AS user_id, 
                                           firstName first_name, 
                                           lastName last_name, 
                                           gender, 
                                           level
                                      FROM staging_events_table
                                     WHERE userId IS NOT NULL;""")




song_table_insert = ("""INSERT INTO songs (song_id, 
                                           title, 
                                           artist_id, 
                                           year, 
                                           duration) 
                                    SELECT DISTINCT so.song_id,
                                           so.title, 
                                           so.artist_id, 
                                           so.year, 
                                           so.duration
                                      FROM staging_events_table se
                                 LEFT JOIN staging_songs_table so
                                        ON se.song = so.title 
                                       AND se.length = so.duration
                                       AND se.artist = so.artist_name
                                     WHERE se.page = 'NextSong'
                                       AND so.song_id IS NOT NULL;""")


artist_table_insert = ("""INSERT INTO artists (artist_id, 
                                               name, 
                                               location, 
                                               latitude, 
                                               longitude) 
                                        SELECT DISTINCT artist_id, 
                                               artist_name, 
                                               artist_location, 
                                               artist_latitude, 
                                               artist_longitude
                                          FROM staging_songs_table;""")


time_table_insert = ("""INSERT INTO time (start_time, 
                                          hour, 
                                          day, 
                                          week, 
                                          month, 
                                          year, 
                                          weekday) 
                                   SELECT DISTINCT start_time,
                                          EXTRACT(hour FROM start_time) AS hour,
                                          EXTRACT(day FROM start_time) AS day,
                                          EXTRACT(week FROM start_time) AS week,
                                          EXTRACT(month FROM start_time) AS month,
                                          EXTRACT(year FROM start_time) AS year,
                                          EXTRACT(dayofweek FROM start_time) AS weekday
                                     FROM songplays;""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

