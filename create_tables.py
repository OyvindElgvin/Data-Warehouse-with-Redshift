import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Uses the drop queries and drops the tables
    """
    for query in drop_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
            print("{:20} dropped".format(query[21:]))
        except psycopg2.Error as e: 
            print("Error: Dropping table")
            print (e)
        
        


def create_tables(cur, conn):
    """
    Uses the create queries and create the tables
    """
    for query in create_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
            print("{:20} created".format(query.split()[5]))
        except psycopg2.Error as e: 
            print("Error: Creating table")
            print (e)
        


def main():
    """
    This main function starts a cursor and a connection to the 
    cluster, and executes the drop_tables and create_tables functions.
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