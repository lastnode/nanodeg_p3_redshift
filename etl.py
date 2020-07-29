import configparser
import psycopg2
from sql_queries import staging_table_queries, final_table_queries
import argparse
from etl_utils import run_query


def load_staging_tables(cur, conn):

    """
    Uses Redshift's COPY function -
    https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
    to insert JSON data from a s3 bucket into two staging tables:

    1) staging_events
    2) staging_songs

    Runs the list of queries in `staging_table_queries` in the `sql_queries`
    module through etl_utils.run_query() which executes them in turn.

    Paramters:
    cur (psycopg2.cursor()) - cursor of the (Postgres) db
    conn (psycopg2.connect()) - connection to the (Postgres) db

    Returns:
    None
    """

    for query in staging_table_queries:
        run_query(cur, conn, query)


def insert_final_tables(cur, conn):

    """
    Extracts data from our two staging tables, transforms the data
    into the format required for our analytics to be run and inserts
    them into the five following tables:

    1) songplays
    2) users
    3) songs
    4) artists
    5) time

    Runs the list of queries in `drop_table_queries` in the `sql_queries`
    module through etl_utils.run_query() which executes them in turn.

    Paramters:
    cur (psycopg2.cursor()) - cursor of the (Postgres) db
    conn (psycopg2.connect()) - connection to the (Postgres) db

    Returns:
    None
    """

    for query in final_table_queries:
        run_query(cur, conn, query)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("""
    host={}
    dbname={}
    user={}
    password={}
    port={}
    """.format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Check for command line flags to see if the user wanted to skip
    # loading either the staging or final tables. This can be useful
    # when testing, since loading data into staging tables can take
    # some time.

    parser = argparse.ArgumentParser(
        prog='etl.py',
        description="""ETL Script that extracts data from
            s3 buckets and loads them into tables.""")

    parser.add_argument(
        '-s', '--skip-staging',
        action='store_true',
        help="""Skip extracing data from s3 bucket and loading them
        into the staging tables.""")

    parser.add_argument(
        '-f', '--skip-final',
        action='store_true',
        help="""Skip extracing data from the staging tables and
        loading them into the final tables.""")

    args, _ = parser.parse_known_args()

    if args.skip_staging:
        print("Not loading s3 bucket data into staging tables.")
    else:
        load_staging_tables(cur, conn)

    if args.skip_final:
        print("Not loading staging table data into final tables.")
    else:
        insert_final_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
