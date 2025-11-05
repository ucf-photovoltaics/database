# -*- coding: utf-8 -*-
"""
Created on Wed May 7 18:22:40 2025

NSF Access operations module.

Author: Albert

"""

import json
import boto3
import pandas as pd
from pathlib import Path
from botocore.client import Config
from botocore.exceptions import ClientError
from typing import List
import os


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
            access_key=self.keys["access_key_id"],
            secret_key=self.keys["secret_access_key"],
            endpoint_url=self.keys["endpoint_url"],
        )

    def _load_keys(self, key_file: str) -> dict:
        with open(key_file, "r") as f:
            return json.load(f)

    def _create_s3_client(self, access_key: str, secret_key: str, endpoint_url: str):
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )

    @staticmethod
    def _batch_process(
        input_files: pd.Series | list, batch_size: int = 200
    ) -> List[List[str]]:
        batches = []

        if input_files is None:
            return []

        try:
            input_list = input_files.tolist()
        except Exception:
            input_list = list(input_files)

        if not input_list:
            return []

        batches = [
            input_list[i : i + batch_size]
            for i in range(0, len(input_list), batch_size)
        ]
        return batches

    def put_files(
        self,
        input_files: pd.Series,
        bucket_name: str,
        prefix: str = "",
        return_report: bool = False,
    ) -> dict | None:
        """
        Upload files to a given S3-compatible bucket.

        Args:
            input_files: pandas Series that contains file paths to upload.
            bucket_name: Name of the bucket to upload to.
            return_report: If True, returns a report dictionary of upload statuses.
            prefix: (Optional) Prefix to add to the uploaded file keys.
        """
        report = {}

        batches = self._batch_process(input_files)

        for batch in batches:
            for file_path in batch:
                file_path = Path(file_path)
                filename = file_path.name
                key = f"{prefix.rstrip('/')}/{filename}" if prefix else filename

                if not file_path.exists():
                    print(f"[MISSING FILE] {file_path} not found.")
                    report[filename] = "missing"
                    continue

                try:
                    with open(file_path, "rb") as f:
                        data = f.read()
                        self.s3_client.put_object(
                            Bucket=bucket_name,
                            Key=key,
                            Body=data,
                            ContentLength=len(data),
                        )
                        print(f"[SUCCESS] Uploaded: {file_path.name}")
                        report[filename] = "success"
                except ClientError as e:
                    print(f"[UPLOAD ERROR] {file_path.name}: {e}")
                    report[filename] = f"client_error: {str(e)}"
                except Exception as e:
                    print(f"[ERROR] {file_path.name}: {e}")
                    report[filename] = f"error: {str(e)}"

        return report if return_report else None

    def list_files(self, bucket_name: str, prefix: str = "") -> List[str]:
        """
        List all objects in a bucket.

        Args:
            bucket_name: Name of the bucket.
            prefix: Optional prefix filter.

        Returns:
            List: Object keys in the bucket.
        """
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            all_keys = []
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        all_keys.append(obj["Key"])
            return all_keys
        except ClientError as e:
            print(f"[LIST ERROR] {bucket_name}: {e}")
            return []

    def get_files(
        self, bucket_name: str, prefix: str = "", return_report: bool = False
    ):
        """
        Downloads all files from a given S3 bucket (optionally filtered by prefix)
        and saves them locally in a folder named <bucket_name>_download/.

        Args:
            bucket_name: Name of the S3 bucket.
            prefix: Directory filter for files in the bucket.
            return_report: If True, returns a dictionary of download statuses.
        """

        download_dir = f"{bucket_name}_download"
        Path(download_dir).mkdir(parents=True, exist_ok=True)

        file_objs = self.list_files(bucket_name, prefix)
        report = {}

        if not file_objs:
            print(f"[INFO] No files found in {bucket_name} with prefix {prefix}")

        for key in file_objs:
            # Skip keys that are folder placeholders (end with '/')
            if key.endswith("/"):
                print(f"[SKIP] Skipping folder key: {key}")
                report[key] = "skipped (folder)"
                continue

            # Optionally skip keys without an extension (treat as folders)
            if not os.path.splitext(key)[1]:
                print(f"[SKIP] Skipping key with no file extension: {key}")
                report[key] = "skipped (no extension)"
                continue

            local_path = Path(download_dir) / key
            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(local_path, "wb") as f:
                    self.s3_client.download_fileobj(bucket_name, key, f)
                print(f"[DOWNLOADED] {key} -> {local_path}")
                report[key] = "downloaded"
            except ClientError as e:
                msg = e.response.get("Error", {}).get("Message", str(e))
                print(f"[CLIENT ERROR] {key} : {msg}")
                report[key] = f"client_error: {msg}"
            except Exception as e:
                print(f"[ERROR] {key}: {e}")
                report[key] = f"error: {str(e)}"
        return report if return_report else None
