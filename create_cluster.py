import boto3
import configparser
import json
import pandas as pd
import time

from botocore.exceptions import ClientError

# GLOBAL VARIABLES
VERBOSE  = 1      # Set verbosity to 0|1|2 (default=0)
redshift = None  # A boto3 service client for Redshif
__all__  = [redshift]  # Specifies the variables that can be imported by other modules.


################## THIS IS A LINE OF 80 CHARACTERS ############################

def get_aws_region():
    """
    Returns the name of AWS region as a string.
    """
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    region = config.get("AWS","AWS_REGION")
    return ("\'{}\'").format(region)

def get_cluster_name():
    """
    Returns the name of the cluster as a string.
    """
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    return config.get("CLUSTER","CLUSTER_NAME")

def get_cluster_status():
    """
    Returns the status of the cluster as a string or None.
    """
    global redshift
    if redshift is None:
        print("WARNING: The cluster is not available or not created.")
        return None
    else:
        return redshift.describe_clusters(ClusterIdentifier=get_cluster_name())['Clusters'][0]['ClusterStatus']

def get_usr_key():
    """
    Returns the AWS user key as a string.
    """
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    return config.get("USR","USR_KEY")

def get_usr_secret():
    """
    Returns the AWS user secret as a string.
    """
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    return config.get("USR","USR_SECRET")    
    
def get_cluster_endpoint(rs_client):
    """
    Retrieves the endpoint address of a cluster (see myDWH.cfg).
    
    Args:
      rs_client (boto3.client): a boto3 client for that cluster. 
    Returns:
      cluster_endpoint (str): the cluster endpoint adresss.
    """
    global VERBOSE
    cluster_endpoint = ''
    try:
        cluster_props = rs_client.describe_clusters(ClusterIdentifier=get_cluster_name())['Clusters'][0]
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ClusterNotFound':
            print("ERROR: The cluster \'%s\' does not exist!\n\tCannot continue..." % get_cluster_name())
            exit()
        else:
            print("Unexpected error: %s" % ce)
            exit()
    else:
        cluster_endpoint = cluster_props['Endpoint']['Address']
        if VERBOSE > 1:
            print("CLUSTER_ENDPOINT = ", cluster_endpoint)
    return cluster_endpoint


def get_cluster_role_arn():
    """
    Retrieves the role AWS resource name of the current cluster (see myDWH.cfg).
     
    Returns:
      cluster_endpoint (str): the cluster endpoint adresss.
    """
    global VERBOSE
    global redshift
    if redshift is None:
        print("WARNING: The cluster IS not available or not created.")
        return None
    
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=get_cluster_name())['Clusters'][0]
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ClusterNotFound':
            print("ERROR: The cluster \'%s\' does not exist!" % get_cluster_name())
        else:
            print("Unexpected error: %s" % ce)
    else:
        cluster_role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
        if VERBOSE > 2:
            print("\tCLUSTER_ROLE_ARN :: ", cluster_role_arn)
    return cluster_role_arn


def get_db_connect_parms():
    """
    Retrives the parameters required to setup a database connection.
    
    Note that this call will also create a boto3 service client for Redshift.
    
    Returns:
        A 5-tuple (name, user, password, host, port) defined as follows:
        - dbname (string): the database name
        - dbuser (string): the user name used to authenticate
        - dbpassword (string): the password used to authenticate
        - dbhost (string): the database host address
        - dbport (string): connection port number        
    """
    global redshift
    # Load cluster parameters from configuration file
    config = configparser.ConfigParser()
    config.read('myDWH.cfg')
    ## Retrieve USER-related  parameters
    USR_KEY                = config.get('USR','USR_KEY')
    if USR_KEY is None:
        print("ERROR: Unspecified access key. Please enter your AWS access key in the \'myDWH.cfg\` file!")
        quit()
    USR_SECRET             = config.get('USR','USR_SECRET')
    if USR_SECRET is None:
        print("ERROR: Unspecified secret key. Please enter your AWS secret key in the \'myDWH.cfg\` file!")
        quit()
    ## Retrieve AWS-related  parameters
    AWS_REGION             = config.get('AWS','AWS_REGION')
    # Retreive the connection-related parameters
    dbname                 = config.get("CLUSTER","CLUSTER_DB_NAME")
    dbuser                 = config.get("CLUSTER","CLUSTER_DB_USER")
    dbpassword             = config.get("CLUSTER","CLUSTER_DB_PASSWORD")
    dbport                 = config.get("CLUSTER","CLUSTER_DB_PORT")       
    # Create a client for Redshif
    try:
        redshift = boto3.client('redshift', region_name=AWS_REGION,
                                aws_access_key_id=USR_KEY,
                                aws_secret_access_key=USR_SECRET)
    except Exception as e:
        print("Error while trying to create a client for Redshift: %s" % e)

    # Retrieve the cluster endpoint
    dbhost = get_cluster_endpoint(redshift)
    
    # Debug trace
    if VERBOSE > 0:
        print("\nDATABASE CONNETION PARAMETERS:")
        df = pd.DataFrame({"Param": ["CLUSTER_DB_NAME",     "CLUSTER_DB_USER",
                                     "CLUSTER_DB_PASSWORD", "CLUSTER_ENDPOINT",
                                     "CLUSTER_DB_PORT"],
                           "Value": [ dbname,                dbuser,
                                      dbpassword,            dbhost,
                                      dbport ]
                          })
        print(df.to_string() + "\n")
    
    # Return the 5-tuple
    return (dbname, dbuser, dbpassword, dbhost, dbport)
    
    

def main():
    global VERBOSE
    
    # STEP-1:  Load cluster parameters from 'myDWH.cfg'
    config = configparser.ConfigParser()
    config.read_file(open('myDWH.cfg'))

    # Retrieve the USER-related  parameters
    USR_KEY                = config.get('USR','USR_KEY')
    USR_SECRET             = config.get('USR','USR_SECRET')

    # Retrieve the AWS-related  parameters
    AWS_REGION             = config.get('AWS','AWS_REGION')

    # Retrieve the CLUSTER-related  parameters
    CLUSTER_NAME           = config.get("CLUSTER","CLUSTER_NAME")
    CLUSTER_TYPE           = config.get("CLUSTER","CLUSTER_TYPE")
    CLUSTER_NODE_TYPE      = config.get("CLUSTER","CLUSTER_NODE_TYPE")
    CLUSTER_NODE_COUNT     = config.get("CLUSTER","CLUSTER_NODE_COUNT")

    CLUSTER_DB_NAME        = config.get("CLUSTER","CLUSTER_DB_NAME")
    CLUSTER_DB_USER        = config.get("CLUSTER","CLUSTER_DB_USER")
    CLUSTER_DB_PASSWORD    = config.get("CLUSTER","CLUSTER_DB_PASSWORD")
    CLUSTER_DB_PORT        = config.get("CLUSTER","CLUSTER_DB_PORT")

    # Retrieve the IAM-related parameters
    AWS_ROLE_ARN           = config.get("IAM_ROLE", "AWS_ROLE_ARN")

    (CLUSTER_DB_USER, CLUSTER_DB_PASSWORD, CLUSTER_DB_NAME)

    if VERBOSE > 0:
        df = pd.DataFrame({"Param":
                           ["CLUSTER_TYPE", "CLUSTER_NODE_COUNT", "CLUSTER_NODE_TYPE", "CLUSTER_NAME", "CLUSTER_DB_NAME", "CLUSTER_DB_USER", "CLUSTER_DB_PASSWORD", "CLUSTER_DB_PORT", "AWS_ROLE_ARN", "AWS_REGION"],
                           "Value":
                           [ CLUSTER_TYPE ,  CLUSTER_NODE_COUNT ,  CLUSTER_NODE_TYPE ,  CLUSTER_NAME ,  CLUSTER_DB_NAME ,  CLUSTER_DB_USER ,  CLUSTER_DB_PASSWORD ,  CLUSTER_DB_PORT ,  AWS_ROLE_ARN ,  AWS_REGION ]
                          })
        print(df)

    # STEP-2: Create clients for EC2, S3, IAM, and Redshift
    #
    # Note-1: To use Boto3, we must indicate which services we are going to 
    #         use. In our case it will be: ec2, s3, iam and redshift.
    #
    # Note-2: If you have the AWS CLI installed, then you can use the AWS 
    #         configure command to configure the credentials file instead of 
    #         passing them as parameters.
    if VERBOSE > 0:
        print("Create clients for EC2, S3, IAM, and Redshift")
        
    ec2 = boto3.resource('ec2',
                           region_name=AWS_REGION,
                           aws_access_key_id=USR_KEY,
                           aws_secret_access_key=USR_SECRET
                        )

    s3 = boto3.resource('s3',
                           region_name=AWS_REGION,
                           aws_access_key_id=USR_KEY,
                           aws_secret_access_key=USR_SECRET
                       )

    iam = boto3.client('iam',aws_access_key_id=USR_KEY,
                         aws_secret_access_key=USR_SECRET,
                         region_name=AWS_REGION
                      )

    redshift = boto3.client('redshift',
                           region_name=AWS_REGION,
                           aws_access_key_id=USR_KEY,
                           aws_secret_access_key=USR_SECRET
                           ) 

    ## STEP-3: Create an IAM Role #############################

    ### STEP-3.1: CREATE IAM ROLE #############################
    if VERBOSE > 0:
        print("Creating IAM Role")

    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=AWS_ROLE_ARN,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                   'Version': '2012-10-17'})
        )
    except Exception as e:
        print("\nWARNING: A role with similar name most likely already exists.")
        if VERBOSE > 0:
            print(e)
    else:
        if VERBOSE > 0:
            print("\tFYI - Details of the current role with name \''%s\':" % dwhRole['Role']['RoleName'])
            print(dwhRole)
            print()

    ### STEP-3.2: ATTACH POLICY ###############################
    response = iam.attach_role_policy(RoleName=AWS_ROLE_ARN,                                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                     )['ResponseMetadata']['HTTPStatusCode']
    if VERBOSE > 0:
        print("Attaching Policy - Allow access to all Amazon S3 buckets (ReadOnly)") 
        # See also - https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum-create-role.html
        print('Response: %i\n' % response)

    ### STEP-3.3: RETRIEVE THE IAM ROLE ARN ###################
    roleArn = iam.get_role(RoleName=AWS_ROLE_ARN)['Role']['Arn']
    if VERBOSE > 0:
        print('Getting the IAM role ARN')
        print(roleArn + '\n')
        
    ## STEP-4:  Create the Redshift cluster
    # - Create a [RedShift Cluster](https://console.aws.amazon.com/redshiftv2/home)
    # - For complete arguments to `create_cluster`, see [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)
    if VERBOSE > 0:
        print("Creating Redshift cluster")

    try:
        response = redshift.create_cluster(
            # Parameters for HW
            ClusterType   = CLUSTER_TYPE,
            NodeType      = CLUSTER_NODE_TYPE,
            NumberOfNodes = int(CLUSTER_NODE_COUNT),
            # Parameters for Identifiers & Credentials
            #  - DBName (string) -
            #      The name of the first database to be created when the cluster is created.
            #  - ClusterIdentifier (string) –
            #      A unique identifier for the cluster. You use this identifier to refer to
            #      the cluster for any subsequent cluster operations such as deleting or modifying.
            #  - MasterUsername (string) –
            #      The user name associated with the admin user for the cluster that is being created.
            DBName             = CLUSTER_DB_NAME,
            ClusterIdentifier  = CLUSTER_NAME,
            MasterUsername     = CLUSTER_DB_USER,
            MasterUserPassword = CLUSTER_DB_PASSWORD,       
            # Parameters for Roles (to allow s3 access)
            #   - IamRoles (list) – 
            #      A list of IAM roles that can be used by the cluster to access other AWS services.
            #      You must supply the IAM roles in their Amazon Resource Name (ARN) format.
            IamRoles=[roleArn]
        )
        if VERBOSE > 0:
            print(response)
    except Exception as e:
        print(e)

    ## STEP-5:  Wait for the cluster to become available
    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    while redshift.describe_clusters(ClusterIdentifier=CLUSTER_NAME)['Clusters'][0]['ClusterStatus'] != 'available':
        if VERBOSE > 0:
            print("Waiting for custer to come up...")
        time.sleep(5)
    print('Cluster is available')

    # Describe the created cluster
    cluster_props = redshift.describe_clusters(ClusterIdentifier=CLUSTER_NAME)['Clusters'][0]

    if VERBOSE > 0:
        prettyRedshiftProps(cluster_props)

    # Display the cluster endpoint and role ARN 
    CLUSTER_ENDPOINT = cluster_props['Endpoint']['Address']
    CLUSTER_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
    print("\nFYI: This is the created cluster ENDPOINT and cluster Role ARN")
    print("\tCLUSTER_ENDPOINT :: ", CLUSTER_ENDPOINT)
    print("\tCLUSTER_ROLE_ARN :: ", CLUSTER_ROLE_ARN)

    exit()
    quit()
    
    ## STEP-6: Open a TCP port to access the cluster endpoint

    if VERBOSE > 0:
        print("Opening a TCP port to the endpoint")

    vpc = ec2.Vpc(id=cluster_props['VpcId'])

    # Get the list of security groups.
    #  - A security group acts as a firewall for the traffic to and from the resources in your VPC.
    default_sg = list(vpc.security_groups.all())[0]
    if VERBOSE > 0:
        print("Default security group: %s" % default_sg)    

    try:
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(CLUSTER_DB_PORT),
            ToPort=int(CLUSTER_DB_PORT)
        )
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()

