import time

"""
Support module for both `create_tables.py` and `etl.py`.
Currently contains the main run_query() function used
throughout both those scripts.
"""


def run_query(cur, conn, query):

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
    print("Running query:")
    print(query)

    cur.execute(query)
    conn.commit()

    print(time.time() - time_start)