import boto3
import pandas as pd
import os
import io
from datetime import datetime
from io import StringIO


s3 = boto3.resource(
    service_name = 's3',
    region_name = 'ap-northeast-1',
    aws_access_key_id = os.environ.get("de_project_user1_access_key"),
    aws_secret_access_key = os.environ.get("de_project_user1_secret_key")
)
s3_client = boto3.client(
    service_name = 's3',
    region_name = 'ap-northeast-1',
    aws_access_key_id = os.environ.get("de_project_user1_access_key"),
    aws_secret_access_key = os.environ.get("de_project_user1_secret_key")
)

bkt = 'postgres-to-snowflake'


response = s3_client.list_objects_v2(Bucket = "postgres-to-snowflake", Prefix = "raw/")
files = response.get("Contents")

for file in files:
    file_key = f"{file['Key']}"
    #print(file_key)
    print(os.path.basename(file_key))

    file_key = f"{file['Key']}"
    response = s3_client.get_object(Bucket = "postgres-to-snowflake", Key = file_key)
    obj = s3.Object('postgres-to-snowflake', '{}'.format(file_key))
    data=obj.get()['Body'].read()
    df = pd.read_csv(io.BytesIO(data))
    print(df)

    # df['Last Modified'] = pd.Timestamp('now').strftime("%Y/%m/%d %H:%M:%S:%f")

    # csv_buffer = StringIO()
    # df.to_csv(csv_buffer, index=False)
    # file_name = os.path.basename(file_key)
    # print("Upload Process Started for\n\n\n", file_name)
    # response = s3_client.put_object(Body = csv_buffer.getvalue(), Bucket = "postgres-to-snowflake", Key = 'raw/{}.csv'.format(file_name))
    # print("Transaction process completed for ", file_name) 
    

