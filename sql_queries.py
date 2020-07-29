import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist text,
auth text,
first_name text,
gender char(1),
item_session numeric,
last_name text,
length numeric,
level text,
location text,
method text,
page text distkey,
registration numeric,
session_id numeric,
song text,
status numeric,
ts timestamp sortkey,
user_agent text,
user_id numeric
)
""")

staging_songs_table_create = ("""
CREATE  TABLE IF NOT EXISTS staging_songs (
num_songs numeric,
artist_id text distkey,
artist_latitude numeric,
artist_longitude numeric,
artist_location text,
artist_name text,
song_id text,
title text,
duration numeric,
year numeric
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id bigint IDENTITY(0, 1),
start_time timestamp references time(start_time) sortkey not null,
user_id int references users(user_id) distkey not null,
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
user_id int distkey sortkey not null,
first_name text,
last_name text,
gender text,
level text,
PRIMARY KEY (user_id))
""")

song_table_create = ("""
CREATE TABLE songs (
song_id text distkey sortkey not null,
title text,
artist_id text,
year int,
duration double precision,
PRIMARY KEY (song_id))
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id text distkey sortkey not null,
name text,
location text,
latitude double precision,
longitude double precision,
PRIMARY KEY (artist_id))
""")

time_table_create = ("""
CREATE TABLE time (
start_time timestamp distkey sortkey not null,
hour int,
day int,
week int,
month int,
year int,
weekday int,
PRIMARY KEY (start_time))
""")

# STAGING TABLES

# Copy code adapted from 'Cloud Data Warehouses' module exercises.
# Redshift timeformat code from -
# https://stackoverflow.com/questions/28496065/epoch-to-timeformat-yyyy-mm-dd-hhmiss-while-redshift-copy

staging_events_copy = ("""
copy staging_events from {}
iam_role {}
json {}
timeformat as 'epochmillisecs'
blanksasnull
emptyasnull
region 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
iam_role {}
format as json 'auto'
blanksasnull
emptyasnull
region 'us-west-2';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
select
    distinct staging_events.ts as start_time,
    staging_events.user_id,
    staging_events.level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.session_id,
    staging_events.location,
    staging_events.user_agent
from staging_events

inner join staging_songs on
    staging_events.artist = staging_songs.artist_name and
    staging_events.song = staging_songs.title

where staging_events.page = 'NextSong'
""")

# Redshift UPSERT equivalent via Udacity mentor answer -
# https://knowledge.udacity.com/questions/276119

user_table_insert = ("""
insert into users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
select
    distinct user_id,
    staging_events.first_name,
    staging_events.last_name,
    staging_events.gender,
    staging_events.level
from staging_events

where staging_events.page = 'NextSong' and
user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
insert into songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
select
    distinct staging_songs.song_id,
    staging_songs.title,
    staging_songs.artist_id,
    staging_songs.year,
    staging_songs.duration
from staging_songs

where staging_songs.song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
insert into artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
select
    distinct staging_songs.artist_id,
    staging_songs.artist_name as name,
    staging_songs.artist_location as location,
    staging_songs.artist_latitude as latitude,
    staging_songs.artist_longitude as longitude
from staging_songs

where staging_songs.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

# Using Redshit's EXTRACT function -
# https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html

time_table_insert = ("""
insert into time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
select
    distinct staging_events.ts as start_time,
    extract (hour from staging_events.ts) as hour,
    extract (day from staging_events.ts) as day,
    extract (week from staging_events.ts) as week,
    extract (month from staging_events.ts) as month,
    extract (year from staging_events.ts) as year,
    extract (weekday from staging_events.ts) as weekday
from staging_events
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
    ]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
    ]

staging_table_queries = [
    staging_events_copy,
    staging_songs_copy
    ]

final_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
    ]
