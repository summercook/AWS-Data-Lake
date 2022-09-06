import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS  users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    session_id int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int);

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1),
    start_time datetime NOT NULL,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar,
    PRIMARY KEY (songplay_id));
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar NOT NULL sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    PRIMARY KEY (user_id))
diststyle auto;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL,
    title varchar,
    artist_id varchar sortkey,
    year int,
    duration float,
    PRIMARY KEY (song_id))
diststyle auto;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL,
    name varchar sortkey,
    location varchar,
    latitude float,
    longitude float,
    PRIMARY KEY (artist_id))
diststyle auto;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time datetime NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int,
    PRIMARY KEY (start_time))
diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""

    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role=arn:aws:iam::996415416856:role/myRedshiftRole'
    compupdate off region 'us-west-2'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""")

staging_songs_copy = (
"""
    copy staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role=arn:aws:iam::996415416856:role/myRedshiftRole'
    compupdate off region 'us-west-2'
    FORMAT AS JSON 'auto'
""")


# FINAL TABLES


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
SELECT DATEADD(second, ts /1000, '19700101') as start_time, userid, level, song_id, artist_id, session_id, location, useragent
FROM staging_events se
JOIN
(SELECT songs.song_id, artists.artist_id, songs.title, artists.name,songs.duration
FROM songs
JOIN artists
ON songs.artist_id = artists.artist_id) AS ss
ON (ss.title = se.song
AND ss.name = se.artist)
WHERE se.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT DISTINCT userid, firstname, lastname, gender, level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration) \
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude) \
SELECT  artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday) \
SELECT DISTINCT start_time, DATE_PART(hour, start_time) as hour, DATE_PART(day, start_time) as day,  DATE_PART(week, start_time) as week, DATE_PART(month, start_time) as month, DATE_PART(year, start_time) as year, DATE_PART(weekday, start_time) as weekday
FROM
(SELECT DATEADD(second, ts /1000, '19700101') as start_time
FROM staging_events)
""")


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
