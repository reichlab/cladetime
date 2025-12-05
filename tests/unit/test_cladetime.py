from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from freezegun import freeze_time

from cladetime.cladetime import CladeTime
from cladetime.exceptions import CladeTimeDataUnavailableError, CladeTimeDateWarning, CladeTimeInvalidURLError


def test_cladetime_no_args(patch_s3_for_tests):
    # patch_s3_for_tests: Mocks S3 sequence data to prevent failures from missing historical versions
    # Use 2025 date (after 2025-09-29 cutoff)
    with freeze_time("2025-12-13 16:21:34", tz_offset=-4):
        ct = CladeTime()
        expected_date = datetime.now(timezone.utc)
    assert ct.tree_as_of == expected_date
    assert ct.sequence_as_of == expected_date


@pytest.mark.parametrize(
    "sequence_as_of, tree_as_of, expected_sequence_as_of, expected_tree_as_of",
    [
        (
            # sequence_as_of set to current date, tree_as_of uses valid date within hub range
            None,
            "2024-10-15",
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
            datetime(2024, 10, 15, 11, 59, 59, tzinfo=timezone.utc),
        ),
        (
            # future dates revert to current date
            "2063-12-21",
            None,
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
        ),
        (
            # sequence and tree both have future dates, both revert to current date
            "2063-12-21",
            "2074-07-13",
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
        ),
    ],
)
def test_cladetime_as_of_dates(sequence_as_of, tree_as_of, expected_sequence_as_of, expected_tree_as_of, patch_s3_for_tests):
    # patch_s3_for_tests: Mocks S3 sequence data to prevent failures from missing historical versions
    # Note: Only testing valid/future dates now. Old dates (before 2025-09-29 for sequence,
    # before 2024-10-09 for tree) are tested in test_cladetime_data_unavailable_* tests
    with freeze_time("2025-07-13 16:21:34"):
        ct = CladeTime(sequence_as_of=sequence_as_of, tree_as_of=tree_as_of)

    assert ct.sequence_as_of == expected_sequence_as_of
    assert ct.tree_as_of == expected_tree_as_of


@pytest.mark.parametrize("bad_date", ["2022-12-32"])
def test_cladetime_invalid_date_format(bad_date, patch_s3_for_tests):
    # Test invalid date format (2022-12-32 is invalid - December doesn't have 32 days)
    # This should trigger a warning and default to current date
    with pytest.warns(CladeTimeDateWarning):
        CladeTime(sequence_as_of=bad_date, tree_as_of=bad_date)


def test_cladetime_future_date(patch_s3_for_tests):
    # patch_s3_for_tests: Mocks S3 sequence data to prevent failures from missing historical versions
    with pytest.warns(CladeTimeDateWarning):
        CladeTime(sequence_as_of="2063-07-13")
    with pytest.warns(CladeTimeDateWarning):
        CladeTime(tree_as_of="2063-07-13")


def test_cladetime_urls(s3_setup, test_config, patch_s3_for_tests):
    """Test CladeTime URL generation with mocked S3.

    Note: This test uses test_config which mocks S3 setup but cannot bypass
    the new date validation in CladeTime setters. Using None (current date)
    which is always valid.
    """
    s3_client, bucket_name, s3_object_keys = s3_setup

    mock = MagicMock(return_value=test_config, name="CladeTime._get_config_mock")

    with patch("cladetime.CladeTime._get_config", mock):
        with freeze_time("2025-10-15 00:00:00"):
            ct = CladeTime(sequence_as_of=None)  # Use current date which is always valid
            for url in [ct.url_sequence, ct.url_sequence_metadata]:
                parsed_url = urlparse(url)
                key = parsed_url.path.strip("/")
                version_id = parse_qs(parsed_url.query)["versionId"][0]
                object = s3_client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
                # With mocked S3 and current date, should get version 4
                assert object.get("Metadata") =={"version": "4"}

            # Current date is after ncov metadata availability
            assert ct.url_ncov_metadata is not None


def test_cladetime_ncov_metadata(s3_setup, s3_object_keys, test_config, patch_s3_for_tests):
    s3_client, bucket_name, s3_object_keys = s3_setup
    mock = MagicMock(return_value=test_config, name="CladeTime._get_config_mock")

    # Mock the Hub fallback to raise ValueError (no archive available)
    mock_fallback = MagicMock(side_effect=ValueError("No archive found"))

    with patch("cladetime.CladeTime._get_config", mock):
        with patch("cladetime.sequence._get_metadata_from_hub", mock_fallback):
            with freeze_time("2025-10-15 00:00:00"):  # Use 2025 date after cutoff
                ct = CladeTime()
                version_id = parse_qs(urlparse(ct.url_ncov_metadata).query)["versionId"][0]
                # Generate a presigned URL for the specific version of the object
                presigned_url = s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket_name, "Key": s3_object_keys["ncov_metadata"], "VersionId": version_id},
                    ExpiresIn=3600,
                )
                ct.url_ncov_metadata = presigned_url

    assert ct.ncov_metadata.get("nextclade_dataset_name_full") == "nextstrain/sars-cov-2/wuhan-hu-1/orfs"
    assert ct.ncov_metadata.get("nextclade_version_num") == "3.8.2"

    # Test that when URL returns 404, ncov_metadata falls back and returns {}
    # (mock fallback will raise ValueError, resulting in empty dict)
    with patch("cladetime.sequence._get_metadata_from_hub", mock_fallback):
        with patch("cladetime.sequence._get_session") as mock_session:
            mock_response = MagicMock()
            mock_response.ok = False
            mock_response.status_code = 404
            mock_session.return_value.get.return_value = mock_response
            ct.url_ncov_metadata = "https://httpstat.us/404"
            assert ct.ncov_metadata == {}


@pytest.mark.skip("Need moto fixup to test S3 URLs")
def test_cladetime_sequence_metadata(test_config):
    mock = MagicMock(return_value=test_config, name="CladeTime._get_config_mock")
    with patch("cladetime.CladeTime._get_config", mock):
        ct = CladeTime()
    assert isinstance(ct.sequence_metadata)


def test_cladetime_sequence_metadata_no_url(test_config):
    mock = MagicMock(return_value=test_config, name="CladeTime._get_config_mock")
    with patch("cladetime.CladeTime._get_config", mock):
        ct = CladeTime()
    ct.url_sequence_metadata = None

    with pytest.raises(CladeTimeInvalidURLError):
        ct.sequence_metadata


def test_cladetime_sequence_as_of_before_data_availability():
    """Test that CladeTime raises error for sequence_as_of before 2025-09-29."""
    with pytest.raises(CladeTimeDataUnavailableError) as excinfo:
        CladeTime(sequence_as_of="2024-10-30")

    assert "Sequence data is not available before 2025-09-29" in str(excinfo.value)
    assert "Nextstrain S3 only retains approximately 7 weeks" in str(excinfo.value)
    assert "GitHub issue #185" in str(excinfo.value)


def test_cladetime_tree_as_of_before_data_availability(patch_s3_for_tests):
    """Test that CladeTime raises error for tree_as_of before 2024-10-09."""
    # Use a valid sequence_as_of date (within S3 retention) but invalid tree_as_of
    with pytest.raises(CladeTimeDataUnavailableError) as excinfo:
        with freeze_time("2025-10-15"):
            CladeTime(sequence_as_of="2025-10-15", tree_as_of="2024-08-01")

    assert "Reference tree metadata is not available before 2024-10-09" in str(excinfo.value)
    assert "variant-nowcast-hub archives" in str(excinfo.value)
    assert "GitHub issue #185" in str(excinfo.value)


@pytest.mark.parametrize(
    "sequence_date, tree_date, expected_error",
    [
        # Test various dates before data availability
        ("2023-05-01", None, "Sequence data is not available"),
        ("2024-01-15", None, "Sequence data is not available"),
        ("2025-09-28", None, "Sequence data is not available"),  # One day before cutoff
    ],
)
def test_cladetime_data_unavailable_various_dates(sequence_date, tree_date, expected_error):
    """Test that various dates before data availability raise appropriate errors."""
    with pytest.raises(CladeTimeDataUnavailableError) as excinfo:
        CladeTime(sequence_as_of=sequence_date, tree_as_of=tree_date)

    assert expected_error in str(excinfo.value)
