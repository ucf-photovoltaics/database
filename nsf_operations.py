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


    def uri_generator(self, bucket_name:str):
        pass

    def upload_files(self, input:DataFrame.Series,bucket_name:str)->None:
        """Column from a dataframe (Series)"""
        pass

    def download_files(self, bucket_name:str, output: DataFrame.Series)->None:
        """Column from a dataframe (Series)"""
        pass

    """Source -> destination, can be buckets for both param, input==> path, (globus)"""
    def transfer_files(source, dest):
        pass