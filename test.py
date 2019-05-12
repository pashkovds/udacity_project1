import psycopg2
import pandas as pd
from sql_queries import test_project_queries


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")

    for i,query in enumerate(test_project_queries):
        print(str(i) + ')  Query to execute:')
        print(query)
        print('\n')
        print('Execution result:')
        print(pd.read_sql(query, con = conn))
        print('\n')
    
    conn.close()

    
if __name__ == "__main__":
    main()