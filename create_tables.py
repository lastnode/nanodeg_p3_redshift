import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from etl_utils import run_query


def drop_tables(cur, conn):

    """
    Connects to the Amazon Redshift database specifiied in 'dwh.cfg'
    in order to drop tables remaining from previous ETL processes.
  
    Passes a list of `drop table` queries to etl_utils.run_query() which
    executes them in turn.

    Paramters:
    cur (psycopg2.cursor()) - cursor of the (Postgres) db
    conn (psycopg2.connect()) - connection to the (Postgres) db

    Returns:
    None
    """

    for query in drop_table_queries:
        run_query(cur, conn, query)
             

def create_tables(cur, conn):

    """
    Connects to the Amazon Redshift database specifiied in 'dwh.cfg'
    in order to set up the required staging and final data tables.

    Passes a list of `create table` queries to etl_utils.run_query() which
    executes them in turn.

    Paramters:
    cur (psycopg2.cursor()) - cursor of the (Postgres) db
    conn (psycopg2.connect()) - connection to the (Postgres) db

    Returns:
    None
    """

    for query in create_table_queries:
        run_query(cur, conn, query)


def main():

    """
    Main function of this script that creates the tables. 
    
    Connects to the Postgres server
    and passes the connection and the cursor to the following functions
    so they can set up the tables required by the `etl.py` script:

    drop_tables()
    creat_tables()

    Parameters:
    None

    Returns:
    None
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()