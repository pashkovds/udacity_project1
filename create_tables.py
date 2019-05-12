import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    '''This fuction do following 5 steps:
       1) Try to connect to local postgres server with credentials student@127.0.0.1:studentdb.
       2) After conncetion check if skparklifydb exists in studentdb. 
       3) If so drop existing database and create new empty database with the same name.
       4) Then disconnects from current db (studentdb) and connects to already created sparkifydb ( student@127.0.0.1:sparkifydb).
       5) For user convinience sparkifydb cursor was created and returned in pair with connection.
   
       Returns:
            cur: Cursor for queries execution 
            conn: Connection to postgres database with autocommit=True
    '''
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    '''Drop all project tables from drop_table_queries in sql_queries.py
    '''
    for query in drop_table_queries:
        print(query+'\n\n')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''Execute create_table_queries from sql_quries.py and create all tables in project
    '''
    for query in create_table_queries:
        print(query+'\n\n')
        cur.execute(query)
        conn.commit()


def main():
    '''Create sparklifydb database, drop and create tables required for project
    '''
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()