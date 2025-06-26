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
from typing import List


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
        self.keys = self._load_keys(key_file)
        self.s3_client = self._create_s3_client(
            access_key=self.keys['access_key_id'],
            secret_key=self.keys['secret_access_key'],
            endpoint_url=self.keys['endpoint_url']
        )

    
    def _load_keys(self, key_file:str) -> dict:
        with open(key_file, 'r') as f:
            return json.load(f)
    
    def _create_s3_client(self, access_key:str, secret_key:str, endpoint_url:str):
        return boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
    def _get_s3_client_from_file(self, key_file: str):
        keys = self._load_keys(key_file)
        return self._create_s3_client(
            access_key=keys['access_key_id'],
            secret_key=keys['secret_access_key'],
            endpoint_url=keys['endpoint_url']
        )
    
    def _batch_process(input_files: pd.Series)-> List[List[str]]:
        input_list = input_files.tolist()
        batch_size = 200
        batches = []

        if len(input_list) >= batch_size:
            for i in range(0, len(input_list), batch_size):
                batch = input_list[i:i+batch_size]
                batches.append(batch)
            else:
                batchs.append(input_list)
            return batches
    def upload_files(self, input_files: pd.Series, bucket_name:str, prefix: str = "", return_report: bool = False) -> dict | None:
        """
        Upload files to a given S3-compatible bucket.

        Args:
            input_files: pandas Series that contains file paths to upload.
            bucket_name: Name of the bucket to upload to.
        """
        report = {}

        batches = _batch_process(input_files)

        for batch in batches:
            for file_path in batch:
                file_path = Path(file_path)
                filename = file_path.name
                key = f"{prefix}/{filename}" if prefix else file_path.name

                if not file_path.exists():
                    print(f"[MISSING FILE] {file_path} not found.")
                    report[filename] = 'missing'
                    continue

                try:
                    with open(file_path, 'rb') as f:
                        data = f.read()
                        self.s3_client.put_object(
                            Bucket=bucket_name,
                            Key=key,
                            Body=data,
                            ContentLength=len(data)
                        )
                        print(f"[SUCCESS] Uploaded: {file_path.name}")
                        report[filename] = 'success'
                except ClientError as e:
                    print(f"[UPLOAD ERROR] {file_path.name}: {e}")
                    report[filename] = f'client_error: {str(e)}'
                except Exception as e:
                    print(f"[ERROR] {file_path.name}: {e}")
                    report[filename] = f'error: {str(e)}'

        return report if return_report else None
    
    def list_files(self, bucket_name: str, prefix: str = "") -> pd.Series:
        """
        List all objects in a bucket.

        Args:
            bucket_name: Name of the bucket.
            prefix: Optional prefix filter.

        Returns:
            pd.Series: Object keys in the bucket.
        """
        try:
            ### Uses a paginator to get objects by page to not run into the limit (1000 objects) for one query
            ### Makes subsequent queries to list_objects_v2 until all objects are retrieved.
            paginator = self.s3_client.get_paginator("list_objects_v2")
            all_keys = []
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page['Contents']:
                        all_keys.extend(obj["Key"])
            return pd.Series(all_keys)
        except ClientError as e:
            print(f"[LIST ERROR] {bucket_name}: {e}")
            return pd.Series([])

#Create a subclass for the timeseries data
class Time_Series(NSF_DB):
    pass
"""
Notes:
    --- Transfer between bucket to bucket (boto3) == Trying to make it more maintainable
    --- Add a feedback loop
    --- Upload feature 
        1. Add a prefix (folder) inside the staging area in the bucket (Done).
        2. Uses batch processing to make the upload feature faster.
    --- Subclass for accessing timeseries data
"""