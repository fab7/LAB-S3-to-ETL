import boto3
import configparser
import json
import pandas as pd
import time
from create_cluster import get_cluster_endpoint, get_cluster_name

# #### Set verbosity to 0|1|2 (default=0)
VERBOSE = 0


################## THIS IS A LINE OF 80 CHARACTERS ############################
        
def main():
    global VERBOSE

    # STEP-1:  Load cluster parameters from 'myDWH.cfg'
    config = configparser.ConfigParser()
    config.read_file(open('myDWH.cfg'))
    
    # Retrieve the USER-related  parameters
    USR_KEY     = config.get('USR','USR_KEY')
    USR_SECRET  = config.get('USR','USR_SECRET')

    # Retrieve the AWS-related  parameters
    AWS_REGION  = config.get('AWS','AWS_REGION')

    # Retrieve the IAM-related parameters
    AWS_ROLE_ARN = config.get("IAM_ROLE", "AWS_ROLE_ARN")

    # STEP-2: Create a client for Redshift and IAM
    if VERBOSE > 0:
        print("Create clients for Redshift and IAM")

    redshift = boto3.client('redshift',
                           region_name           = AWS_REGION,
                           aws_access_key_id     = USR_KEY,
                           aws_secret_access_key = USR_SECRET
                           )

    iam = boto3.client('iam',
                       aws_access_key_id     = USR_KEY,
                       aws_secret_access_key = USR_SECRET,
                       region_name           = AWS_REGION
                      )
    
    # STEP-3: Clean up the running cluster
    try:
         # Check if cluster exists by trying to retrieve the endpoint address
        dbhost = get_cluster_endpoint(redshift)
    except Exception as e:
        print("Unexpected error: %s" % e)
        exit()
    
    print("\nWARNING: This is going to delete the Redshift cluster \'{}\' which endpoint addres is \n\t \'{}\'. ".format(get_cluster_name(), dbhost))
    
    key = input("\nARE YOU SURE (Y,n): ")
    if key != 'Y':
        print("aborting here...")
        quit()
   
    # Bye Bye - Here we go and get ride of the cluster... 
    redshift.delete_cluster( ClusterIdentifier=get_cluster_name(),  SkipFinalClusterSnapshot=True)
 
    # STEP-4: Wait for the cluster to be deleted
    try:
        while redshift.describe_clusters(ClusterIdentifier=get_cluster_name())['Clusters'][0]['ClusterStatus'] != 'available':
            print("Cluster is being deleted...")
            time.sleep(5)
    except Exception as e:
        print(e)
    print('Cluster is deleted')

    # STEP-5: Detach the IAM Role and Policy
    try:
        iam.detach_role_policy(RoleName=AWS_ROLE_ARN, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=AWS_ROLE_ARN)
    except Exception as e:
        print(e)
    
    print("DONE: \n\tIAM is detached and Role ARN i sdelete.")
    

if __name__ == "__main__":
    main()


