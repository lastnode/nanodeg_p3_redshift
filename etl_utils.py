import time
import psycopg2
import configparser

"""
Support module for both `create_tables.py` and `etl.py`.
Currently contains the create_connection() and run_query()
functions used throughout both those scripts.
"""


def create_connection(config_file):

    """
    Takes the path to `.cfg` file as an argument, and uses that
    information to create a connection to a Redshift (Postgres)
    database. Returns the connection and the cursor so other functions
    can make use of them.

    Paramters:
    config_file - path to a `.cfg` file that contains connection
    details for the Postgres database.

    Returns:
    cur (psycopg2.cursor()) - cursor of the (Postgres) db
    conn (psycopg2.connect()) - connection to the (Postgres) db
    """

    config = configparser.ConfigParser()
    config.read(config_file)

    try:
        connection = psycopg2.connect("""
        host={}
        dbname={}
        user={}
        password={}
        port={}
        """.format(*config['CLUSTER'].values()))

    except psycopg2.Error as error:
        print("Error: Could not make connection to the Postgres database.")
        print(error)

    try:
        cursor = connection.cursor()

    except psycopg2.Error as error:
        print("Error: Could not get cursor.")
        print(error)

    return(cursor, connection)


def run_query(cursor, connection, query):

    """
    Uses the Postgres connection and cursor passed in to
    run the query that has been passed in as well. Also
    prints the query to STDOUT and prints the time taken
    for the query to execute as well.

    To check execution time, we currently use time.time()
    but we may want to implement timeit.timeit() at a later
    date.

    Paramters:
    cur (psycopg2.cursor()) - cursor of the (Postgres) db
    conn (psycopg2.connect()) - connection to the (Postgres) db

    Returns:
    None
    """

    time_start = time.time()

    cursor.execute(query)

    connection.commit()

    print("Query:", end=" ")

    print(query)

    try:
        rows = cursor.fetchall()

        print("Result:", end=" ")

        for row in rows:
            print(row)

    except psycopg2.Error as error:
        print(error)        

    print("Execution time:", time.time() - time_start, '\n')
