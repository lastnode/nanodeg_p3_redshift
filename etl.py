import argparse
from etl_utils import run_query, create_connection
from sql_queries import staging_table_queries, final_table_queries

"""
Script that connects to Redshift and creates both the staging
and final tables required by the project.
"""


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

    """
    Main function of this script that populates the staging
    and final tables.

    Calls `etl_utils.py.create_connection()` in order to connect
    to the Postgres server. Then it passes the connection and the
    cursor to the following functions so they can populate the staging
    and final tables required by the project:

    load_staging_tables()
    insert_final_tables()

    If required, both these functions can be skipped individually using the
    following command line flags:

    --skip-staging
    --skip-final

    This is a useful feature for the ETL process, since loading data from
    s3 in particular can be a time-intensive task and you may not need to do
    so if troubleshooting another part of the pipeline.

    Parameters:
    None

    Returns:
    None
    """

    cursor, connection = create_connection("dwh.cfg")

    # Check for command line flags to see if the user wanted to skip
    # loading either the staging or final tables.

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
        load_staging_tables(cursor, connection)

    if args.skip_final:
        print("Not loading staging table data into final tables.")
    else:
        insert_final_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
