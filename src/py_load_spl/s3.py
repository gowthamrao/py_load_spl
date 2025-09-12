import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from .config import S3Settings

logger = logging.getLogger(__name__)


class S3Uploader:
    """Handles uploading files to an AWS S3 bucket."""

    def __init__(self, settings: S3Settings):
        if not settings.bucket:
            raise ValueError("S3 bucket name must be configured to use S3Uploader.")
        self.settings = settings
        self.s3_client = boto3.client("s3")

    def upload_directory(self, local_path: Path) -> str:
        """Uploads all files from a local directory to S3.

        Args:
            local_path: The local directory containing files to upload.

        Returns:
            The S3 URI of the uploaded prefix.
        """
        if not local_path.is_dir():
            raise ValueError(f"Provided path '{local_path}' is not a directory.")

        # This assertion helps mypy understand that self.settings.bucket is not None here,
        # even though it's typed as Optional, because we check it in __init__.
        assert self.settings.bucket is not None

        logger.info(
            "Starting upload of directory '%s' to s3://%s/%s",
            local_path,
            self.settings.bucket,
            self.settings.prefix,
        )

        file_count = 0
        for file_path in local_path.glob("**/*"):
            if file_path.is_file():
                s3_key = f"{self.settings.prefix}/{file_path.name}"
                try:
                    self.s3_client.upload_file(
                        str(file_path), self.settings.bucket, s3_key
                    )
                    logger.debug(
                        "Successfully uploaded %s to %s", file_path.name, s3_key
                    )
                    file_count += 1
                except ClientError as e:
                    logger.error(
                        "Failed to upload %s to S3: %s",
                        file_path.name,
                        e,
                        exc_info=True,
                    )
                    raise

        logger.info(
            "Successfully uploaded %d files to s3://%s/%s",
            file_count,
            self.settings.bucket,
            self.settings.prefix,
        )
        return f"s3://{self.settings.bucket}/{self.settings.prefix}"
