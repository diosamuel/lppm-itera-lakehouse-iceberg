import os
from functools import wraps
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()
class SetupMinioS3:
    def __init__(self, endpoint_url, access_key=None, secret_key=None, bucket=None):
        self.endpoint_url = endpoint_url or os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "admin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "password")
        self.bucket = bucket
        self.client = None

    @staticmethod
    def check_bucket(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                self.client.head_bucket(Bucket=self.bucket)
                kwargs["bucket_exists"] = True
            except ClientError:
                kwargs["bucket_exists"] = False
            return func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def check_file(func):
        @wraps(func)
        def wrapper(self, filename, *args, **kwargs):
            try:
                self.client.head_object(Bucket=self.bucket, Key=filename)
                kwargs["file_exists"] = True
            except ClientError:
                kwargs["file_exists"] = False
            return func(self, filename, *args, **kwargs)

        return wrapper

    @check_bucket
    @check_file
    def upload(self, filename, filepath, bucket_exists=False, file_exists=False):
        if not bucket_exists:
            return {
                "status": False,
                "message": f"Bucket '{self.bucket}' does not exist.",
            }

        if file_exists:
            return {"status": False, "message": "file exists"}
        self.client.upload_file(filepath, self.bucket, filename)
        return {"status": "success", "bucket": self.bucket, "key": filename}

    @check_bucket
    @check_file
    def load(self, filename, bucket_exists=False, file_exists=False):
        if not bucket_exists:
            return {
                "status": False,
                "message": f"Bucket '{self.bucket}' does not exist.",
            }
        if not file_exists:
            return {
                "status": False,
                "message": f"File '{filename}' does not exist in bucket '{self.bucket}'.",
            }
        response = self.client.get_object(Bucket=self.bucket, Key=filename)
        content = response["Body"].read()
        return {
            "status": True,
            "content": content,
            "path": f"s3a://{self.bucket}/{filename}",
        }

    @check_bucket
    @check_file
    def read_meta(self, filename, bucket_exists=False, file_exists=False):
        if not bucket_exists:
            return {
                "status": False,
                "message": f"Bucket '{self.bucket}' does not exist.",
            }
        if not file_exists:
            return {
                "status": False,
                "message": f"File '{filename}' does not exist in bucket '{self.bucket}'.",
            }
        response = self.client.head_object(Bucket=self.bucket, Key=filename)
        metadata = {
            "content_type": response.get("ContentType"),
            "content_length": response.get("ContentLength"),
            "last_modified": response.get("LastModified"),
            "etag": response.get("ETag"),
            "metadata": response.get("Metadata", {}),
        }
        return metadata

    @check_bucket
    def list_file(self, bucket_exists):
        if not bucket_exists:
            return {
                "status": False,
                "message": f"Bucket '{self.bucket}' does not exist.",
            }

        response = self.client.list_objects_v2(Bucket=self.bucket)
        filename = []
        for obj in response.get("Contents", []):
            filename.append(obj["Key"])
        return filename

    def initialize(self):
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        return self
