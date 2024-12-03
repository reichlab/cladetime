from datetime import datetime, timezone
from pathlib import Path

import boto3
import pytest
import requests
from freezegun import freeze_time
from moto import mock_aws

from cladetime.util.config import Config


@pytest.fixture
def moto_file_path() -> Path:
    """
    Return path to the unit test files.
    """
    moto_file_path = Path(__file__).parent.joinpath("data").joinpath("moto_fixture")
    return moto_file_path


@pytest.fixture(scope="function")
def demo_mode(monkeypatch):
    "Set demo mode to True for tests using the Nextstrain 100K sequence files."
    demo_mode = "true"
    monkeypatch.setenv("CLADETIME_DEMO", demo_mode)
    yield demo_mode


@pytest.fixture
def test_sequences():
    """Return a set of sequences for testing.

    These sequences have clade assignments that changed between
    2024-08-02 and 2024-11-07, so this is a good set for testing clade
    assignments over time.
    """
    file_name = "test_sequences_updated.fasta"
    sequences = [
        "USA/VA-CDC-LC1109961/2024",
        "USA/MD-CDC-LC1110088/2024",
        "USA/FL-CDC-LC1109983/2024",
    ]

    return (file_name, set(sequences))


@pytest.fixture
def s3_object_keys():
    return {
        "sequence_metadata_zst": "data/metadata.tsv.zst",
        "sequence_metadata_xz": "data/metadata.tsv.xz",
        "sequences_xz": "data/sequences.fasta.xz",
        "ncov_metadata": "data/metadata_version.json",
    }


@pytest.fixture
def mock_session(mocker):
    """Session mock for testing functions that use requests.Session"""
    mock_session = mocker.patch.object(requests, "Session", autospec=True)
    mock_session.return_value.__enter__.return_value = mock_session
    return mock_session


@pytest.fixture
def s3_setup(moto_file_path, s3_object_keys):
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

        # Add versioned sequence, sequence metadata, and ncov metadata test objects
        versions = ["2023-01-01 03:05:01", "2023-02-05 03:33:06", "2023-02-05 14:33:06", "2023-03-22 22:55:12"]
        for i, version in enumerate(versions, start=1):
            extra_args = {"Metadata": {"version": str(i)}}
            # use freezegun to override system date, which in
            # turn sets S3 object version LastModified date
            with freeze_time(version):
                for file in moto_file_path.iterdir():
                    s3_client.upload_file(file, bucket_name, f"data/{file.name}", ExtraArgs=extra_args)

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
    test_config.nextstrain_genome_metadata_key = s3_object_keys["sequence_metadata_zst"]
    test_config.nextstrain_genome_sequence_key = s3_object_keys["sequences_xz"]
    test_config.nextstrain_ncov_metadata_key = s3_object_keys["ncov_metadata"]

    return test_config
