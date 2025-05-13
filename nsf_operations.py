# -*- coding: utf-8 -*-
"""
Created on Wed May 7 18:22:40 2025

NSF Access operations module.

Author: Albert

"""

import json
import boto3
import pandas as pd
import os
from botocore.client import Config, ClientError


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
        Currently just based on Amazon S3 services.
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
    
    # No need to create URI's manually use presigned_uri's since the bucket is private
    def generate_presigned_url(self, bucket_name, object_key, expiration=3600):
        try:
            return self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket':bucket_name, 'key':object_key},
                ExpiresIn=expiration
            )
        except ClientError as e:
            print(f"Failed to generate presigned URL for {object_key}: {e}")
        

    def upload_files(self, input_files:pd.Series, bucket_name:str)->None:

        for file_path in input_files.to_list():
            file_name = os.path.basename(file_path)

            try:
                self.s3_client.upload_file(file_path, bucket_name, file_name)
                print(f"Upload of {file_name} succesfull")
            except ClientError as e:
                print(f"Error uploading {file_name}:{e}")

    def download_files(self, bucket_name:str, output: pd.Series)->None:
        """Column from a dataframe (Series)"""
        pass

    """Source -> destination, can be buckets for both param, input==> path, try to implement (globus)"""
    def transfer_files(source, dest):
        pass
