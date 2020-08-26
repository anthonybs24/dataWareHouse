import configparser
import psycopg2
import boto3
import time
import json


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Create IAM client
iam = boto3.client ('iam', region_name = config.get("CLUSTER","REGION"), aws_access_key_id = config.get("CLUSTER","KEY"), \
                    aws_secret_access_key = config.get("CLUSTER", "SECRET"))

# Create Redshift client
redshift = boto3.client ('redshift', region_name = config.get("CLUSTER","REGION"), aws_access_key_id = config.get("CLUSTER","KEY"), \
                         aws_secret_access_key = config.get("CLUSTER", "SECRET"))

# Create dwh s3 access role and attach 'AmazonS3ReadOnlyAccess' policy. Return role ARN
def create_iam_role():
    try: 
        # Create the IAM role
        dwhRole = iam.create_role(Path='/', RoleName = config.get("IAM_ROLE","ROLE_NAME"), \
                                  AssumeRolePolicyDocument = json.dumps(\
                                                                        {"Version": "2012-10-17", \
                                                                         "Statement": [{"Effect": "Allow", \
                                                                                        "Principal": {"Service": "redshift.amazonaws.com"}, \
                                                                                        "Action": "sts:AssumeRole"}]\
                                                                        }), \
                                  Description = 'Allow redshift clusters to call AWS')
    
        # Attach policy
        iam.attach_role_policy(RoleName = config.get("IAM_ROLE","ROLE_NAME"), PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        
        # Store role ARN to config 'ARN'
        config['IAM_ROLE']['ARN'] = iam.get_role(RoleName = config.get("IAM_ROLE","ROLE_NAME"))['Role']['Arn']
        with open ('dwh.cfg','w') as configfile:
            config.write(configfile)
        
    except Exception as e:
        print (e)

# Create Redshift Cluster and Open an incoming TCP port to access the cluster endpoint
def create_cluster():
    try:
        """
        # Get list of all clusters
        clusters = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER","CLUSTER_ID"))['Clusters']
        
        # Check if cluster with the same Id already exists or not. If not, go ahead to create one
        clusterExists = false;
        i = 0
        while (clusterExists == false and i < len(clusters)):
            if clusters[i][ClusterIdentifier] == config.get("CLUSTER","CLUSTER_ID"):
                clusterExists = true
            i += 1
            
        # Create cluster
        if clusterExists == false:
        """
        redshift.create_cluster(ClusterType = config.get("CLUSTER","CLUSTER_TYPE"), NodeType = config.get("CLUSTER","NODE_TYPE"), \
                                    NumberOfNodes = int(config.get("CLUSTER","NUM_NODES")), \
                                    ClusterIdentifier = config.get("CLUSTER","CLUSTER_ID"), DBName = config.get("CLUSTER","DB_NAME"), \
                                    MasterUsername = config.get("CLUSTER","DB_USER"), MasterUserPassword = config.get("CLUSTER","DB_PASSWORD"), \
                                    Port = int(config.get("CLUSTER","DB_PORT")), IamRoles = [config.get('IAM_ROLE','ARN')])
        
        # Check status of the cluster
        clusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER","CLUSTER_ID"))['Clusters'][0]
        while clusterProps['ClusterStatus'] != "available":
            # Sleep for 30s prior to check status of the cluster
            time.sleep(15)
            clusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("CLUSTER","CLUSTER_ID"))['Clusters'][0]
            
        # Store cluster endpoint to config 'HOST'
        config['CLUSTER']['HOST'] = clusterProps['Endpoint']['Address']
        with open ('dwh.cfg','w') as configfile:
            config.write(configfile)
            
        # Once cluster is available, we open an incoming TCP port to access the cluster endpoint
        # Create EC2 client
        ec2 = boto3.resource ('ec2', region_name = config.get("CLUSTER","REGION"), aws_access_key_id = config.get("CLUSTER","KEY"), \
                                 aws_secret_access_key = config.get("CLUSTER", "SECRET"))

        vpc = ec2.Vpc(id=clusterProps['VpcId'])
        defaultSecurityGroup = list(vpc.security_groups.all())[0]
        defaultSecurityGroup.authorize_ingress(GroupName= 'default', CidrIp='0.0.0.0/0', IpProtocol='TCP', \
                                                FromPort=int(config.get("CLUSTER","DB_PORT")), ToPort=int(config.get("CLUSTER","DB_PORT")))
    
    except Exception as e:
        print (e)

# Delete cluster & IAM role
def delete_cluster_and_role ():
    try:
        # Delete cluster
        response = redshift.delete_cluster( ClusterIdentifier=config.get("CLUSTER", "CLUSTER_ID"), SkipFinalClusterSnapshot=True)

        # Detach policy and delete role
        iam.detach_role_policy(RoleName=config.get('IAM_ROLE','ROLE_NAME'), PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=config.get('IAM_ROLE','ROLE_NAME'))
        
    except Exception as e:
        print (e)
        