import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from create_dwh_cluster import create_iam_role, create_cluster


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Create IAM Role and cluster
    try:
        # Create IAM Role
        create_iam_role()
         
        # Create cluster
        create_cluster()
        
        # Create connection to cluster
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER','HOST'), config.get('CLUSTER','DB_NAME'), \
                                                                                config.get('CLUSTER','DB_USER'), config.get('CLUSTER','DB_PASSWORD'),\
                                                                                config.get('CLUSTER','DB_PORT')))
        cur = conn.cursor()
    
        # Drop tables if they already exist
        drop_tables(cur, conn)
    
        # create tables
        create_tables(cur, conn)
        
        # Close connection
        conn.close()
        
    except Exception as e:
        print (e)


if __name__ == "__main__":
    main()