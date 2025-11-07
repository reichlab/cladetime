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
    """
    Set demo mode to True for testing.

    This fixture activates CladeTime's demo mode, which uses the Nextstrain
    100k dataset instead of the entire universe of SARS-CoV-2 sequences.

    Use with caution: the 100K dataset is compressed using LSTD, which
    follows a different code path than the full dataset normally used by
    Cladetime (which is compressed using ZSTD and is read in batches
    using biobear).
    """
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


@pytest.fixture
def mock_s3_sequence_data():
    """
    Mock _get_s3_object_url to return synthetic version IDs and URLs
    for sequence data files, preventing ValueError when Nextstrain S3
    no longer has historical versions.

    This fixture addresses test failures caused by Nextstrain's October 2025
    cleanup of historical S3 versioned objects. Tests were failing with:
    "ValueError: No version of files/ncov/open/sequences.fasta.zst found before [date]"

    Strategy:
    - Mock sequence files (sequences.fasta.zst, sequences.fasta.xz, metadata.tsv.zst)
      to return synthetic but valid URLs/version IDs
    - Let metadata_version.json calls raise ValueError to test fallback mechanism
    - This allows tests to pass while still testing the Hub fallback functionality

    Returns:
        Function that mocks _get_s3_object_url behavior
    """
    from typing import Tuple

    def mock_get_url(bucket_name: str, object_key: str, date: datetime) -> Tuple[str, str]:
        # For demo mode files (100k), return current non-versioned URLs
        # These tests need to access real current data from Nextstrain
        if "/100k/" in object_key:
            # Return empty version ID and non-versioned URL for current data access
            url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"
            return ("", url)

        # Mock sequence and metadata files - these no longer have historical versions in S3
        # Return synthetic version IDs and URLs so tests can proceed
        if "sequences.fasta" in object_key or "metadata.tsv" in object_key:
            # Generate consistent mock version ID based on date and key
            version_id = f"mock-{date.strftime('%Y%m%d%H%M%S')}-{object_key.replace('/', '-').replace('.', '-')}"
            url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}?versionId={version_id}"
            return (version_id, url)

        # For metadata_version.json, raise ValueError to test fallback mechanism
        # This simulates the real-world scenario where Nextstrain deleted historical metadata
        if "metadata_version.json" in object_key:
            raise ValueError(f"No version of {object_key} found before {date}")

        # For any other files, also raise ValueError (fail safe)
        raise ValueError(f"No version of {object_key} found before {date}")

    return mock_get_url


@pytest.fixture
def mock_hub_fallback():
    """
    Mock _get_metadata_from_hub to return synthetic metadata for historical dates
    before September 2024 when Hub archives don't exist.

    This allows integration tests to pass for dates prior to the Hub's archive start date.
    Returns metadata consistent with what would have been available from Nextstrain S3 and
    variant-nowcast-hub archives at those dates.
    """
    def mock_get_hub_metadata(date: datetime) -> dict:
        # Return synthetic but realistic metadata matching Hub archive structure
        # The Hub archives contain "nextclade_dataset_name" (not "_full")
        # _get_ncov_metadata will add the "_full" key when it sees "SARS-CoV-2"

        # Return different metadata versions based on date to match test expectations
        if date >= datetime(2024, 10, 1, tzinfo=timezone.utc):
            # Later dates get nextclade 3.9.1
            return {
                "nextclade_dataset_name": "SARS-CoV-2",
                "nextclade_dataset_version": "2024-10-17--16-48-48Z",
                "nextclade_version": "nextclade 3.9.1",
                "nextclade_version_num": "3.9.1"
            }
        else:
            # Earlier dates get nextclade 3.8.2
            return {
                "nextclade_dataset_name": "SARS-CoV-2",
                "nextclade_dataset_version": "2024-07-17--12-57-03Z",
                "nextclade_version": "nextclade 3.8.2",
                "nextclade_version_num": "3.8.2"
            }

    return mock_get_hub_metadata


@pytest.fixture
def patch_s3_for_tests(monkeypatch, mock_s3_sequence_data, mock_hub_fallback):
    """
    Apply S3 mocking to all modules that import _get_s3_object_url.

    This fixture must patch all import locations because Python imports
    create separate references. Tests import from multiple locations:
    - cladetime.util.reference (original)
    - cladetime.cladetime (imports and uses directly)
    - cladetime.tree (imports and uses directly)

    Usage in tests:
        def test_something(patch_s3_for_tests):
            # S3 calls will now use mocked data
            ct = CladeTime(sequence_as_of="2024-08-01")
            # ... test assertions ...
    """
    # Patch at all import locations to ensure mocking works everywhere
    monkeypatch.setattr(
        "cladetime.util.reference._get_s3_object_url",
        mock_s3_sequence_data
    )
    monkeypatch.setattr(
        "cladetime.cladetime._get_s3_object_url",
        mock_s3_sequence_data
    )
    monkeypatch.setattr(
        "cladetime.tree._get_s3_object_url",
        mock_s3_sequence_data
    )

    # Also patch the Hub fallback for dates before September 2024
    # Only patch where _get_metadata_from_hub is actually imported
    monkeypatch.setattr(
        "cladetime.util.reference._get_metadata_from_hub",
        mock_hub_fallback
    )
    monkeypatch.setattr(
        "cladetime.sequence._get_metadata_from_hub",
        mock_hub_fallback
    )

    # Return the mock function in case tests need to inspect or modify it
    return mock_s3_sequence_data
