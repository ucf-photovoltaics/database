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

    def __load_keys(self, key_file: str):
        with open(key_file, 'r') as f:
            keys = json.load(f)
        self.access_key_id = keys['access_key_id']
        self.secret_access_key = keys['secret_access_key']
        self.endpoint_url = keys['endpoint_url']

    def __init_s3_client(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
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
    """
    def download_files(self, bucket_name: str, file_keys: pd.Series = None) -> dict:
        
        Downloads files from S3-compatible storage into memory (BytesIO).

        Args:
            bucket_name (str): The name of the S3 bucket.
            file_keys (pd.Series, optional): Object keys to download. If None, downloads all.

        Returns:
            dict: Mapping from filename to BytesIO content.
    
        if file_keys is None or file_keys.empty:
            file_keys = self.__list_bucket_objects(bucket_name)

        downloaded_files = {}
        for object_key in file_keys.to_list():
            try:
                response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
                downloaded_files[object_key] = BytesIO(response['Body'].read())
                print(f"[Success] Downloaded: {object_key}")
            except ClientError as e:
                print(f"[DOWNLOAD ERROR] {object_key}: {e}")
            except Exception as e:
                print(f"[ERROR] {object_key}: {e}")

        return downloaded_files
    """
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
    --- Globus_Sdk -- (Maybet)
    --- Transfer between bucket to bucket (boto3)
    --- Add a feedback loop
"""