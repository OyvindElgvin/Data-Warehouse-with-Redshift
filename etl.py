import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Uses the copy queries to copy the data from the json files in 
    the s3 to the staging tables.  
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("{:20} loaded".format(query.split()[1]))
        except psycopg2.Error as e: 
            print("Error: loading table")
            print(e)


def insert_tables(cur, conn):
    """
    Uses the insert queries to insert and transform the data from 
    the staging tables to the fact and dimension tables.
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("{:20} inserted".format(query.split()[2]))
        except psycopg2.Error as e: 
            print("Error: inserting table")
            print(e)


def main():
    """
    This main function starts a cursor and a connection to the 
    cluster, and executes the load_staging_tables and insert_tables functions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    

    conn.close()


if __name__ == "__main__":
    main()