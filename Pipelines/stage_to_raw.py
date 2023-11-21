#Codes for transporting datas from stage to raw
import boto3
import pandas as pd
import os
import io
from datetime import datetime
from io import StringIO
class stage_to_raw():
    def __init__(self):
        self.s3_client = boto3.client(
            service_name = 's3',
            aws_access_key_id = os.environ.get("de_project_user1_access_key"),
            aws_secret_access_key = os.environ.get("de_project_user1_secret_key")
        )
            
    def transaction(self):
        s3_client = self.s3_client
        response = s3_client.list_objects_v2(Bucket = "postgres-to-snowflake", Prefix = "raw")
        files = response.get("Contents")
        for file in files:
            file_key = file['Key']
            file_name = os.path.basename(file_key)
            response = s3_client.get_object(Bucket = "postgres-to-snowflake", Key = file_key)
            data = response['Body'].read()
            df = pd.read_csv(io.BytesIO(data))
            df['Last Modified'] = pd.Timestamp('now').strftime("%Y/%m/%d %H:%M:%S:%f")
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Body = csv_buffer.getvalue(), Bucket = "postgres-to-snowflake", Key = 'raw/{}'.format(file_name))
            print(df)
                
obj1 = stage_to_raw()
obj1.transaction()

