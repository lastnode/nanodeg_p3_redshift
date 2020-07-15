import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = ""
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""

""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id bigint IDENTITY(1, 1),
start_time timestamp references time(start_time),
user_id int references users(user_id),
level text,
song_id text references songs(song_id),
artist_id text references artists(artist_id),
session_id int,
location text,
user_agent text,
PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
CREATE TABLE users (
user_id int,
first_name text,
last_name text,
gender text,
level text,
PRIMARY KEY (user_id))
""")

song_table_create = ("""
CREATE TABLE songs (
song_id text,
title text,
artist_id text,
year int,
duration double precision,
PRIMARY KEY (song_id))
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id text,
name text,
location text,
latitude double precision,
longitude double precision,
PRIMARY KEY (artist_id))
""")

time_table_create = ("""
CREATE TABLE time (
start_time timestamp,
hour int,
day int,
week_of_year int,
month int,
year int,
weekday int,
PRIMARY KEY (start_time))
""")

# STAGING TABLES

## Copy code adapted from 'Cloud Data Warehouses' module exercises.

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {}
    gzip delimiter ';' compupdate off region 'us-west-2';
""").format(LOG_DATA,DWH_ROLE_ARN,LOG_JSONPATH)

staging_songs_copy = ("""

""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# staging_events_table_drop, staging_songs_table_drop
# staging_events_table_create, staging_songs_table_create,