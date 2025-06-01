# -*- coding: utf-8 -*-
"""
Created on Wed May 7 18:22:40 2025

NSF Access operations module. Currently not tested.

Author: Albert

"""

import json
import boto3
import pandas as pd
from pathlib import Path
from botocore.client import Config, ClientError
from io import BytesIO


class NSF_DB:
    def __init__(self, key_file: str):
        """
        Initialize NSF_DB connection using credentials from key_file.
        key_file should be a JSON file with:
            {
                "access_key_id": "YOUR_ACCESS_KEY",
                "secret_access_key": "YOUR_SECRET_KEY",
                "endpoint_url": "https://YOUR_OSN_ENDPOINT"
            }
        """
        self.__load_keys(key_file)
        self.__init_s3_client()

    def __repr__(self):
        return f"<NSF_DB endpoint={self.endpoint_url}>"

    def __load_keys(self, key_file: str, return_keys: bool = False):
        with open(key_file, 'r') as f:
            keys = json.load(f)
        
        if return_keys:
            return keys
        
        self.access_key_id = keys['access_key_id']
        self.secret_access_key = keys['secret_access_key']
        self.endpoint_url = keys['endpoint_url']

    def __init_s3_client(self, access_key=None, secret_key=None, endpoint_url=None):
        """
        Initialize and return a boto3 client. If params are None, use instance credentials.
        """
        return boto3.client(
            's3',
            endpoint_url=endpoint_url or self.endpoint_url,
            aws_access_key_id=access_key or self.access_key_id,
            aws_secret_access_key=secret_key or self.secret_access_key,
            config=Config(signature_version='s3v4')
        )

    def upload_files(self, input_files: pd.Series, bucket_name: str) -> None:
        """
        Upload files to a given S3-compatible bucket.

        Args:
            input_files: pandas Series that contains file paths to upload.
            bucket_name: Name of the bucket to upload to.
        """
        for file_path in input_files.to_list():
            file_path = Path(file_path)
            if not file_path.exists():
                print(f"[MISSING FILE] {file_path} not found.")
                continue

            try:
                with open(file_path, 'rb') as f:
                    data = f.read()
                    self.s3_client.put_object(
                        Bucket=bucket_name,
                        Key=file_path.name,
                        Body=data,
                        ContentLength=len(data)
                    )
                print(f"[Success] Uploaded: {file_path.name}")
            except ClientError as e:
                print(f"[UPLOAD ERROR] {file_path.name}: {e}")
            except Exception as e:
                print(f"[ERROR] {file_path.name}: {e}")

    def transfer_between_buckets(self, source_bucket, dest_bucket, dest_key_file, prefix)-> None:
        dest_keys = self.__load_keys(dest_key_file, return_keys=True)
        
        pass
    def __list_bucket_objects(self, bucket_name: str) -> pd.Series:
        """
        List all objects in a bucket.

        Args:
            bucket_name: Name of the bucket.

        Returns:
            pd.Series: Object keys in the bucket.
        """
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            all_keys = []
            for page in paginator.paginate(Bucket=bucket_name):
                if "Contents" in page:
                    all_keys.extend(obj["Key"] for obj in page["Contents"])
            return pd.Series(all_keys)
        except ClientError as e:
            print(f"[LIST ERROR] {bucket_name}: {e}")
            return pd.Series([])


"""
Notes:
    --- Download (transfer functions for)
    --- Bucket => HPC
    --- Globus_Sdk -- (L)
    --- Transfer between bucket to bucket (boto3)
    --- Add a feedback loop
"""