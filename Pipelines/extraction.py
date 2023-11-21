#Data extraction codes, in other words, preparing the data

#Finalizing the data codes
import psycopg2
import os
import pandas as pd
import boto3
from io import StringIO



host = os.environ.get("host")
database = os.environ.get("database")
user = os.environ.get("user")
password = os.environ.get("password")
conn = psycopg2.connect(
    host = host, database = database, user = user, password = password
    )
cursor = conn.cursor()

s3_client = boto3.client(
    service_name = 's3',
    region_name = 'ap-northeast-1',
    aws_access_key_id = os.environ.get("de_project_user1_access_key"),
    aws_secret_access_key = os.environ.get("de_project_user1_secret_key")
)

def get_tables():
    cursor.execute('''SELECT "tablename" FROM pg_catalog.pg_tables WHERE schemaname='public';''')
    table_names = cursor.fetchall()    
    for table_name in table_names:
        #Fetching Table's Column Names
        table_name = table_name[0]
        query = '''SELECT * FROM public."{0}";'''.format(table_name)
        cursor.execute(query)

        #Retrieving Table's Column
        table_columns = [desc[0] for desc in cursor.description]
        
        #Retrieving Table Data
        table_data = cursor.fetchall()
        
        #Creating Dataframe using Table data
        df = pd.DataFrame(table_data, columns=table_columns)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        #Pushing dataframe to S3 Bucket
        response = s3_client.put_object(Body = csv_buffer.getvalue(), Bucket = "postgres-to-snowflake", Key = 'stage/{}.csv'.format(table_name))
        print(table_name, "Has been Pushed Successfully.")
        
get_tables()
