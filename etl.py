import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_cluster import get_cluster_role_arn, get_cluster_status, get_aws_region

from create_cluster import get_db_connect_parms

#  Set verbosity to 0|1|2 (default=0)
VERBOSE = 2

def get_log_data_path():
    """
    Returns the path to the LOG data set as a string.
    """
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    return config.get("S3","LOG_DATA")

def get_song_data_path():
    """
    Returns the path to the SONG data set as a string.
    """
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    return config.get("S3","SONG_DATA")

def get_log_json_path():
    """
    Returns the path to the LOG JSON metadata as a string.
    """
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    return config.get("S3","LOG_JSONPATH")


def load_staging_tables(cur, conn):
    """
    Loads the stagging tables by copying the data from a specified S3 bucket
    into the stagging tables defined by the 'copy_table_queries' list.
    
    Args:
      cur (psycopg2.extensions.cursor): 
          A ref to a cursor object for executing SQL commands on the Redshift
          database.
      conn (psycopg2.extensions.connection):
          A ref to a connection object to interact with the database.
    """
    for query in copy_table_queries:
        if "staging_events" in query:
            query = query.format(get_log_data_path(), get_cluster_role_arn(), get_aws_region(), get_log_json_path())
        elif "staging_songs" in query:
            query = query.format(get_song_data_path(), get_cluster_role_arn(), get_aws_region())
        else:
            print("ERROR: Badly formatted query: \'{}\' ".format(query))
            print("\tExpecting the query to contain the sting \'staging_events\' or \'staging_songs\'!")
            quit()
        if VERBOSE:
            print("The following COPY query is going to be issued:" + query)
        try:  
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: {}".format(e))
        
        
def insert_tables(cur, conn):
    """
    Loads the analytics star tables by inserting data from the staging tables.
    
    Args:
      cur (psycopg2.extensions.cursor): 
          A ref to a cursor object for executing SQL commands on the Redshift
          database.
      conn (psycopg2.extensions.connection):
          A ref to a connection object to interact with the database.
    """
    for query in insert_table_queries:
        if VERBOSE:
            print("The following INSERT query is going to be issued:" + query)
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: {}".format(e))
            

################## THIS IS A LINE OF 80 CHARACTERS ############################
        
def main():
    global VERBOSE
       
    # Create a connection object to interact with Redshift and a cursor object 
    # to execute SQL commands on Redshift
    (dbname, dbuser, dbpassword, dbhost, dbport) = get_db_connect_parms()
    if get_cluster_status() != 'available':
        print("You won't be able to connect to cluster because it is not available or it is not created.")
        quit()
    conn = psycopg2.connect("dbname={} user={} password={} host={} port={}".format(dbname, dbuser, dbpassword, dbhost, dbport))   
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()