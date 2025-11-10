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
        self.aws_bucket_prefix = os.getenv('bucket_prefix')
        self.aws_bucket_name = os.getenv('bucket_name')
        
        self.s3 = boto3.client(
            's3',
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key,
            region_name = self.aws_region
        )

        return None


    def put_in_s3(self, file_key, file_content):
        
        file_key_path = f'{self.aws_bucket_prefix}{file_key}.json'
        json_data = json.dumps(file_content)
       
        response = self.s3.put_object(
            Bucket = self.aws_bucket_name,
            Key = file_key_path,
            Body = json_data,
            ContentType = 'application/json'
        )
        return response


