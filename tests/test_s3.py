from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from pytest_mock import MockerFixture

from py_load_spl.config import S3Settings
from py_load_spl.s3 import S3Uploader

# The region needs to be specified for moto
AWS_REGION = "us-east-1"


@pytest.fixture
def s3_settings() -> S3Settings:
    """Fixture for S3 settings."""
    return S3Settings(bucket="test-bucket", prefix="spl_data")


@mock_aws
def test_s3_upload_directory_success(s3_settings: S3Settings, tmp_path: Path) -> None:
    """Verify that files in a directory are uploaded to S3."""
    # 1. Setup: Create mock S3 bucket and local files
    assert s3_settings.bucket is not None
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.create_bucket(Bucket=s3_settings.bucket)

    (tmp_path / "file1.csv").write_text("data1")
    (tmp_path / "file2.csv").write_text("data2")
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "file3.csv").write_text("data3")

    # 2. Action: Run the uploader
    uploader = S3Uploader(s3_settings)
    result_uri = uploader.upload_directory(tmp_path)

    # 3. Assertions
    assert result_uri == f"s3://{s3_settings.bucket}/{s3_settings.prefix}"

    response = s3_client.list_objects_v2(
        Bucket=s3_settings.bucket, Prefix=s3_settings.prefix
    )
    assert response["KeyCount"] == 3

    assert response["Contents"] is not None
    keys = {item["Key"] for item in response["Contents"]}
    assert f"{s3_settings.prefix}/file1.csv" in keys
    assert f"{s3_settings.prefix}/file2.csv" in keys
    assert f"{s3_settings.prefix}/file3.csv" in keys


@mock_aws
def test_s3_upload_empty_directory(s3_settings: S3Settings, tmp_path: Path) -> None:
    """Verify that uploading an empty directory works and uploads nothing."""
    assert s3_settings.bucket is not None
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.create_bucket(Bucket=s3_settings.bucket)

    uploader = S3Uploader(s3_settings)
    uploader.upload_directory(tmp_path)

    response = s3_client.list_objects_v2(Bucket=s3_settings.bucket)
    assert "Contents" not in response


def test_s3_uploader_raises_error_if_bucket_not_set() -> None:
    """Verify S3Uploader raises ValueError if the bucket name is missing."""
    with pytest.raises(ValueError, match="S3 bucket name must be configured"):
        S3Uploader(S3Settings(bucket=None, prefix="spl"))


def test_s3_uploader_raises_error_if_path_is_not_dir(
    s3_settings: S3Settings, tmp_path: Path
) -> None:
    """Verify upload_directory raises ValueError if the path is not a directory."""
    file_path = tmp_path / "not_a_dir.txt"
    file_path.write_text("I am a file")

    uploader = S3Uploader(s3_settings)
    with pytest.raises(
        ValueError, match=f"Provided path '{file_path}' is not a directory."
    ):
        uploader.upload_directory(file_path)


@mock_aws
def test_s3_upload_raises_and_logs_client_error(
    s3_settings: S3Settings, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Verify that a ClientError during upload is logged and re-raised."""
    # 1. Setup
    assert s3_settings.bucket is not None
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.create_bucket(Bucket=s3_settings.bucket)
    (tmp_path / "file1.csv").write_text("data1")

    # 2. Mock the upload_file method to raise an error
    from botocore.exceptions import ClientError

    mock_upload = mocker.patch(
        "boto3.s3.transfer.S3Transfer.upload_file",
        side_effect=ClientError(
            {"Error": {"Code": "500", "Message": "Internal Server Error"}},
            "upload_file",
        ),
    )

    # 3. Action & Assertion
    uploader = S3Uploader(s3_settings)
    with pytest.raises(ClientError):
        uploader.upload_directory(tmp_path)

    # Verify that upload_file was called
    assert mock_upload.called
