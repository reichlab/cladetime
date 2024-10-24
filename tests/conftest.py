from datetime import datetime, timezone

import boto3
import pytest
import requests
from cladetime.util.config import Config
from freezegun import freeze_time
from moto import mock_aws


@pytest.fixture
def s3_object_keys():
    return {
        "sequence_metadata": "data/object-key/metadata.tsv.zst",
        "sequence": "data/object-key/sequences.fasta.zst",
        "ncov_metadata": "data/object-key/metadata_version.json",
    }


@pytest.fixture
def mock_session(mocker):
    """Session mock for testing functions that use requests.Session"""
    mock_session = mocker.patch.object(requests, "Session", autospec=True)
    mock_session.return_value.__enter__.return_value = mock_session
    return mock_session


@pytest.fixture
def s3_setup(s3_object_keys):
    """
    Setup mock S3 bucket with versioned objects that represent testing files for
    sequence data, sequence metadata, and ncov pipeline metadata.
    """
    with mock_aws():
        bucket_name = "versioned-bucket"

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        s3_client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration={
                "CORSRules": [
                    {
                        "AllowedMethods": ["GET"],
                        "AllowedOrigins": ["https://*"],
                        "AllowedHeaders": ["*"],
                        "MaxAgeSeconds": 3000,
                    }
                ]
            },
        )

        for file, object_key in s3_object_keys.items():
            # Upload multiple versions of the object
            versions = [
                ("2023-01-01 03:05:01", f"{file} version 1"),
                ("2023-02-05 03:33:06", f"{file} version 2"),
                ("2023-02-05 14:33:06", f"{file} version 3"),
                ("2023-03-22 22:55:12", f"{file} version 4"),
            ]

            for version_date, content in versions:
                # use freezegun to override system date, which in
                # turn sets S3 object version LastModified date
                with freeze_time(version_date):
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        Body=content,
                    )

        yield s3_client, bucket_name, s3_object_keys


@pytest.fixture
def test_config(s3_setup):
    """
    Return a Config object for use with the s3_setup fixture.
    """
    s3_client, bucket_name, s3_object_keys = s3_setup
    test_config = Config()
    test_config.nextstrain_min_seq_date = datetime(2023, 1, 1).replace(tzinfo=timezone.utc)
    test_config.nextstrain_ncov_bucket = "versioned-bucket"
    test_config.nextstrain_genome_metadata_key = s3_object_keys["sequence_metadata"]
    test_config.nextstrain_genome_sequence_key = s3_object_keys["sequence"]
    test_config.nextstrain_ncov_metadata_key = s3_object_keys["ncov_metadata"]

    return test_config
