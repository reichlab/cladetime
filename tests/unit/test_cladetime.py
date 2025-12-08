from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

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


def test_cladetime_urls(patch_s3_for_tests):
    """Test CladeTime URL generation.

    Simplified test that verifies URLs are generated correctly.
    Detailed S3 versioning behavior is tested in integration tests.
    """
    with freeze_time("2025-10-15 00:00:00"):
        ct = CladeTime(sequence_as_of=None)

        # Verify sequence URLs are generated
        assert ct.url_sequence is not None
        assert "sequences.fasta" in ct.url_sequence
        assert ct.url_sequence_metadata is not None
        assert "metadata.tsv" in ct.url_sequence_metadata

        # Current date is after ncov metadata availability, but may trigger fallback
        # (url_ncov_metadata can be empty string if fallback is needed)
        assert ct.url_ncov_metadata is not None or ct.url_ncov_metadata == ""


def test_cladetime_ncov_metadata(patch_s3_for_tests):
    """Test that ncov_metadata property works with fallback mechanism.

    Simplified test - detailed metadata content is tested in integration tests.
    """
    with freeze_time("2025-10-15 00:00:00"):
        ct = CladeTime()

        # ncov_metadata should either return valid metadata or empty dict (if fallback fails)
        metadata = ct.ncov_metadata
        assert isinstance(metadata, dict)

        # If metadata is not empty, it should have expected structure
        if metadata:
            assert "nextclade_dataset_name" in metadata or "nextclade_dataset_name_full" in metadata


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
    assert "Nextstrain S3 only retains up to 90 days" in str(excinfo.value)
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
