import boto3
import configparser
import pandas as pd
import psycopg2
from sql_queries import drop_table_queries
from sql_queries import create_staging_table_queries, create_star_table_queries
from create_cluster import get_cluster_role_arn, get_cluster_status, get_aws_region
from create_cluster import get_db_connect_parms

#  Set verbosity to 0|1|2 (default=0)
VERBOSE = 2


def retrieve_endpoint_and_role_arn(cluster_client, cluster_name):
    """Return the endpoint address and the role RAN of a named Redshift cluster.
    Parameters:
        cluster_name (string): The name of the Redshift cluster.
        cluster_client (botocore.client.BaseClient): A ref to an AWS boto3 service client.
    Returns:
        (cluster_endpoint (str), cluster_role_arn(str)): A tuple with containing the 
            cluster endpoint adresss and the cluster Role ARN.
    """
    global VERBOSE
    try:
        cluster_props = cluster_client.describe_clusters(ClusterIdentifier=cluster_name)['Clusters'][0]
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ClusterNotFound':
            print("ERROR: The cluster \'%s\' does not exist!" % cluster_name)
        else:
            print("Unexpected error: %s" % ce)
    else:
        cluster_endpoint = cluster_props['Endpoint']['Address']
        cluster_role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
        if VERBOSE > 1:
            print("\nFYI: This is the cluster ENDPOINT and cluster Role ARN")
            print("\tCLUSTER_ENDPOINT :: ", cluster_endpoint)
            print("\tCLUSTER_ROLE_ARN :: ", cluster_role_arn)
    return (cluster_endpoint, cluster_role_arn) 
            
            

def drop_tables(cur, conn):
    """Drop all the tables specified by 'drop_table_queries'.
    Parameters:
        cur (psycopg2.extensions.cursor): A ref to a cursor object for executing SQL
            commands on the Redshift database.
        conn (psycopg2.extensions.connection): A ref to a connection object to interact
            with the database.
    """
    global VERBOSE
    for query in drop_table_queries:
        if VERBOSE > 1:
            print("Execute query: {}".format(query))
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: {}".format(e))


def create_tables(cur, conn):
    """Create all the tables specified by 'create_[type]_table_queries'.
    Parameters:
        cur (psycopg2.extensions.cursor): A ref to a cursor object for executing SQL
            commands on the Redshift database.
        conn (psycopg2.extensions.connection): A ref to a connection object to interact
            with the database.
    """
    global VERBOSE
    for query in create_staging_table_queries:
        if VERBOSE > 1:
            print("Execute query: {}".format(query))
        try:    
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: {}".format(e))
            
    for query in create_star_table_queries:
        if VERBOSE > 1:
            print("Execute query: {}".format(query))
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: {}".format(e))
            quit()

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
    
    # Drop all the tables before starting over
    drop_tables(cur, conn)

    # Create the tables before running the ETL pipeline
    create_tables(cur, conn)

    # Close the connection with the cluster
    conn.close()


if __name__ == "__main__":
    main()
              