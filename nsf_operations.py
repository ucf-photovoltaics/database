# -*- coding: utf-8 -*-
"""
Created on Wed May 7 18:22:40 2025

NSF Access operations module.

Author: Albert

"""


import json
import boto3
from botocore.client import Config

class NSF_DB:
    def __init__(self,key_file):
        """
        Initialize NSF_DB connection using credentials from key_file.
        key_file should be a JSON file with:
            {
                "access_key": "YOUR_ACCESS_KEY",
                "secret_key": "YOUR_SECRET_KEY",
                "endpoint_url": "https://YOUR_OSN_ENDPOINT"
            }
        """
        with open(key_file, 'r') as f:
            keys = json.load(f)

        self.access_key = keys['access_key']
        self.secret_key = keys['secret_key']
        self.endpoint_url = keys['endpoint_url']

        self.s3_client = boto3.client(
                's3',
                endpoint_url = self.endpoint_url,
                aws_access_key = self.access_key,
                aws_secret_access = self.secret_key,
                config=Config(signature_version='s3v4')
        )


    def uri_generator(self,bucket_name:str):
        pass

    def upload_files(self, bucket_name:str)->None:
        pass

    def download_files(self,bucket_name:str)->None:
        pass

    def search_files(self,bucket_name:str)->bool:
        pass
