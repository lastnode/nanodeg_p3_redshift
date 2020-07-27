# Introduction

A part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), this [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) project looks to collect and present user activity information for a fictional music streaming service called Sparkify. To do this, data is gathered from song information and application `.json` log files (which were generated from the [Million Song Dataset](http://millionsongdataset.com/) and from [eventsim](https://github.com/Interana/eventsim) respectively and given to us).

Whereas in [the first iteration of this project](https://github.com/lastnode/nanodeg_p1_songs), the data was stored locally and then loaded into [pandas](https://pandas.pydata.org/) dataframes so they could be filtered before being inserted into a Postgres SQL database, in this version we loaded the data directly into an [Amazon Redshift](https://aws.amazon.com/redshift/) cluster and used [Redshift's COPY function](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) to do the data extraction for us. This was particularly useful because it took full advantage of Redshift's MPP (massively parallel processing) architecture to perform paralell ETL on the JSON files that had to be processed.

# Files
```
- the folder with song information and user log information, all in .json format
- README.md -- this file
- create_tables.py -- creates tables necessary for ETL script
- etl.py - the main ETL script that interacts with Redshift to create staging anf final tables
- sql_queries.py -- a module that create_tables.py and etl.py load to run the SQL queries
- etl_utils.ipy -- a support module that both  `etl.py` and `create_tables.py` rely on
- Dashboard.ipynb -- a Jupyter notebook that functions as a sample analytics dashboard
```

# ETL Scripts

## Setup

In order to run these Python scripts, you will first need to install Python 3 on your computer, and then install the following Python modules via [pip](https://pypi.org/project/pip/) or [anaconda](https://www.anaconda.com/products/individual):

- [psycopg2](https://pypi.org/project/psycopg2/) - a PostgreSQL database adapter for Python.
- [argparse](https://pypi.org/project/argparse/) - a module that allow us to access command line interface (CLI) arguments entered by users.

To install these via `pip` you can run:

`pip install psycopg2 argparse`


## Primary
These are the two primary scripts that will need to be run for this project, in the order that they need to be run.

1) `create_tables.py`  - This script drops any existing tables in Redshift and creates the necesary tables for our `etl.py` script to run. `etl.py` will not execute correctly if you first do not run this script.

2) `etl.py` - This is the main ETL script for the project. It connects to Redshift and creates both the staging and final tables required for the project.

Note that `etl.py` come swith two command line options:

```
  -s, --skip-staging  Skip extracing data from s3 bucket and loading them into the staging tables.
  -f, --skip-final    Skip extracing data from the staging tables and loading them into the final tables.
```

This makes the ETL process more modular and allows for easier data pipeline troubleshooting as well.

## Secondary
3) `sql_queries.py` - This is a module that both `create_tables.py` and `etl.py` load to run the SQL queries needed to both set up the tables required by this project, and then insert data into them. This script is not executed directly.

4) `etl_utils.py` - A support module that contains a `run_query()` that is used by both `etl.py` and `create_tables.py`. Going forward this module can be expanded with other required utility functions.

5) `Dashboard.ipynb.` - A [Jupyter](https://jupyter.org/) notebook that contains sample queries that might be run by an analytics team. It can be expanded on as analysts think about what other queries they would like to run on the data.

# Database Schema
Given that the primary purpose of this project is to show _what songs users are listening to_, the `songplays` table is our fact table, with several other dimension tables feeding into it. Based on the relative simplicity of the relationships in this project, we have opted to organise these tables in a straightforward star schema.

```
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
start_time timestamp references time(start_time) sortkey,
user_id int references users(user_id) distkey,
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
user_id int distkey sortkey,
first_name text,
last_name text,
gender text,
level text,
PRIMARY KEY (user_id))
""")

song_table_create = ("""
CREATE TABLE songs (
song_id text distkey,
title text,
artist_id text,
year int,
duration double precision,
PRIMARY KEY (song_id))
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id text distkey sortkey,
name text,
location text,
latitude double precision,
longitude double precision,
PRIMARY KEY (artist_id))
""")

time_table_create = ("""
CREATE TABLE time (
start_time timestamp distkey sortkey,
hour int,
day int,
week int,
month int,
year int,
weekday int,
PRIMARY KEY (start_time))
""")
```

## Example Queries

Example queries can be found in the `Dashboard.ipynb` Jupyter notebook.