# -*- coding: utf-8 -*-
"""
Created on Wed May 7 18:22:40 2025

NSF Access operations module. Currently not tested.

Author: Albert

"""

import json
import boto3
import pandas as pd
import os
from botocore.client import Config, ClientError
from io import BytesIO
import requests
from globus_sdk.scopes import TransferScopes
from globus_sdk.transfer import TransferClient, TransferData
from pathlib import Path

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
        self.__load_keys(key_file)
        self.__int_s3_client()

    def __load_keys(self, key_file):
        with open(key_file, 'r') as f:
            keys = json.load(f)
        
        self.access_key = keys['access_key']
        self.secret_key = keys['secret_key']
        self.endpoint_url = keys['endpoint_url']
    
    def __int_s3_client(self):
        self.s3_client = boto3.client(
        's3',
        endpoint_url = self.endpoint_url,
        aws_access_key = self.access_key,
        aws_secret_access = self.secret_key,
        config=Config(signature_version='s3v4')
    )
    
    # Only for private buckets, and for user who don't have access to the bucket, grants temporary access to objects
    def __generate_presigned_url(self, bucket_name, object_key, expiration=3600):
        try:
            return self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket':bucket_name, 'key':object_key},
                ExpiresIn=expiration
            )
        except ClientError as e:
            print(f"[URL ERROR] {object_key}: {e}")
            return None
        

    def upload_files(self, input_files:pd.Series, bucket_name:str)->None:
        for file_path in input_files.to_list():
            file_name = os.path.basename(file_path)
            try:
                self.s3_client.upload_file(file_path, bucket_name, file_name)
                print(f"Uploaded: {file_name}")
            except ClientError as e:
                print(f"[UPLOAD ERROR] {file_name}: {e}")

    
    def download_files(self, bucket_name: str, file_keys: pd.Series = None) -> dict:
        """
        Downloads files from S3-compatible storage using presigned URLs.
        Stores files in memory as BytesIO objects.

        Args:
            bucket_name (str): The name of the S3 bucket
            file_keys (pd.Series, optional): Series of object keys (filenames) to download. 
                                            If None, or empty, all objects in the bucket are downloaded.

        Returns:
            dict: Dictionary mapping filenames to BytesIO objects (Memory)
        """

        if file_keys is None or file_keys.empty:
            file_keys = self.__list_bucket_objects(bucket_name)

        downloaded_files = {}
        for object_key in file_keys.to_list():
            try:
                presigned_url = self.generate_presigned_url(bucket_name=bucket_name, object_key=object_key)

                if not presigned_url:
                    print(f"Skipping {object_key} due to URL generation failure.")
                    continue

                response = requests.get(presigned_url)

                if response.status_code == 200:
                    file_obj = BytesIO(response.content)
                    downloaded_files[object_key] = BytesIO(response.content)
                    print(f"Downloaded: {object_key}")
                else:
                    print(f"[DOWNLOAD ERROR] {object_key}:  {response.status_code}")
            except Exception as e:
                print(f"[ERROR] {object_key}: {e}")

        return downloaded_files
    
    def __list_bucket_objects(self, bucket_name: str) -> pd.Series:
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


class GlobusTransferManager:
    def __init__(self, client_id, client_secret, refresh_token, token_file):
        pass

    def __init_transfer_client(self, refresh_token):
        pass

    """Source -> destination, can be buckets for both param, input==>path, try to implement (globus)"""
    def transfer_files(source, dest):
        pass