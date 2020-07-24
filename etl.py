import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import argparse


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Running query:")
        print(query)

        cur.execute(query)
        conn.commit()


def insert_final_tables(cur, conn):
    for query in insert_table_queries:
        print("Running query:")
        print(query)

        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Check for command line flags to see if the user wanted to skip
    # loading either the staging or final tables. This can be useful
    # when testing, since loading data into staging tables can take
    # some time.

    parser = argparse.ArgumentParser(prog='etl.py', description='ETL Script that extracts data from s3 buckets and loads them into tables.')
    parser.add_argument('-s', action='store_true', help='Skip extracing data from s3 bucket and loading them into the staging tables.')
    parser.add_argument('-f', action='store_true', help='Skip extracing data from the staging tables and loading them into the final tables.')


    args, leftovers = parser.parse_known_args()

    if args.s == True:
        print("Not loading data into staging tables.")
    else:
        load_staging_tables(cur, conn)
         
    if args.s == True:
        print("Not loading data into final tables.")
    else:
        insert_final_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()