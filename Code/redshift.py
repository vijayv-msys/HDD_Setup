import boto3
import psycopg2

# Set your AWS credentials (make sure you have AWS CLI configured or provide access key and secret key)
aws_access_key = 'AKIAT4WBONWTDAY5V7DH'
aws_secret_key = 'iiVc2ApqrnIT5/6ZnFoaX4p9zBoXQmcFTX0pgcf/'
region = 'ap-south-1'
cluster_identifier = 'redshift-clus-02'
dbname = 'dev'
user = 'awsuser'
password='Master#1234'
port =5439  # Default Redshift port

# Initialize the Redshift client
redshift_client = boto3.client('redshift', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region)

# Get cluster details
response = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)
cluster_endpoint = response['Clusters'][0]['Endpoint']['Address']

# Create a connection to Redshift
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=cluster_endpoint, port=port)

# Create a cursor
cur = conn.cursor()

# Execute query
query = "SELECT * FROM af_data_table LIMIT 100;"
cur.execute(query)

# Fetch the results
results = cur.fetchall()

# Print the results
for row in results:
    print(row)

