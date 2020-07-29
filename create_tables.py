from etl_utils import run_query, create_connection
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):

    """
    Connects to the Amazon Redshift database specifiied in 'dwh.cfg'
    in order to drop tables remaining from previous ETL processes.

    Runs the list of queries in `create_table_queries` in the `sql_queries`
    module through etl_utils.run_query() which executes them in turn.n.

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

    Runs the list of queries in `create_table_queries` in the `sql_queries`
    module through etl_utils.run_query() which executes them in turn.n.

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

    Calls `etl_utils.py.create_connection()` in order to connect
    to the Postgres server. Then it passes the connection and the
    cursor to the following functions so they can set up the tables
    required by the `etl.py` script:

    drop_tables()
    creat_tables()

    Parameters:
    None

    Returns:
    None
    """

    cursor, connection = create_connection("dwh.cfg")

    drop_tables(cursor, connection)
    create_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
