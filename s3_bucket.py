import boto3
from dotenv import load_dotenv
import os
import logging
from botocore.exceptions import ClientError
import json

load_dotenv()


class aws_s3:

    def __init__(self):
        
        self.aws_access_key_id = os.getenv('aws_access_key_id')
        self.aws_secret_access_key = os.getenv('aws_secret_access_key')
        self.aws_region = os.getenv('aws_region')

        return None

    def _connect_s3_(self):

        s3 = boto3.client(
            's3',
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key,
            region_name = self.aws_region
        )
        return s3


    def put_in_s3(self, file_key, file_content):
        
        file_key_path = f'raw_data/{file_key}.json'
        json_data = json.dumps(file_content)
        s3 = self._connect_s3_()

        response = s3.put_object(
            Bucket = os.getenv('bucket_name'),
            Key = file_key_path,
            Body = json_data,
            ContentType = 'application/json'
        )
        return response


