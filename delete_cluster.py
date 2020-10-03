import configparser
import psycopg2
import boto3

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
        
# Delete cluster & IAM role
def delete_cluster_and_role():
    try:
        # Create IAM client
        iam = boto3.client ('iam', region_name = config.get("CLUSTER","REGION"), aws_access_key_id = config.get("CLUSTER","KEY"), \
                            aws_secret_access_key = config.get("CLUSTER", "SECRET"))

        # Create Redshift client
        redshift = boto3.client ('redshift', region_name = config.get("CLUSTER","REGION"), aws_access_key_id = config.get("CLUSTER","KEY"), \
                                 aws_secret_access_key = config.get("CLUSTER", "SECRET"))


        # Delete cluster
        response = redshift.delete_cluster( ClusterIdentifier=config.get("CLUSTER", "CLUSTER_ID"), SkipFinalClusterSnapshot=True)

        # Detach policy and delete role
        iam.detach_role_policy(RoleName=config.get('IAM_ROLE','ROLE_NAME'), PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=config.get('IAM_ROLE','ROLE_NAME'))
        
    except Exception as e:
        print (e)
        
def main():
    # Create connection to cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER','HOST'), config.get('CLUSTER','DB_NAME'), \
                                                                                config.get('CLUSTER','DB_USER'), config.get('CLUSTER','DB_PASSWORD'),\
                                                                                config.get('CLUSTER','DB_PORT')))
    
    delete_cluster_and_role()
    
    conn.close()


if __name__ == "__main__":
    main()