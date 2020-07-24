import time

def run_query(cur, conn, query):
    time_start = time.time()
    print("Running query:")
    print(query)

    cur.execute(query)
    conn.commit()
    
    print(time.time() - time_start)