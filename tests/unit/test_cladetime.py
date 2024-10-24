from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import dateutil.tz
import pytest
from cladetime.cladetime import CladeTime
from cladetime.exceptions import CladeTimeFutureDateWarning, CladeTimeInvalidDateError, CladeTimeInvalidURLError
from freezegun import freeze_time


def test_cladetime_no_args():
    with freeze_time("2024-12-13 16:21:34", tz_offset=-4):
        ct = CladeTime()
        expected_date = datetime.now(timezone.utc)
    assert ct.tree_as_of == expected_date
    assert ct.sequence_as_of == expected_date


@pytest.mark.parametrize(
    "sequence_as_of, tree_as_of, expected_sequence_as_of, expected_tree_as_of",
    [
        (
            "2024-09-01",
            "2024-01-01",
            datetime(2024, 9, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
        ),
        (
            None,
            "2023-12-21",
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
            datetime(2023, 12, 21, tzinfo=timezone.utc),
        ),
        (
            datetime(2024, 9, 30, 18, 24, 59, 655398),
            None,
            datetime(2024, 9, 30, 18, 24, 59, tzinfo=timezone.utc),
            datetime(2024, 9, 30, 18, 24, 59, tzinfo=timezone.utc),
        ),
        (
            datetime(2024, 2, 22, 22, 22, 22, 222222, tzinfo=dateutil.tz.gettz("US/Eastern")),
            datetime(2024, 2, 22, tzinfo=dateutil.tz.gettz("US/Eastern")),
            datetime(2024, 2, 22, 22, 22, 22, tzinfo=timezone.utc),
            datetime(2024, 2, 22, tzinfo=timezone.utc),
        ),
        (
            "2023-12-21",
            None,
            datetime(2023, 12, 21, tzinfo=timezone.utc),
            datetime(2023, 12, 21, tzinfo=timezone.utc),
        ),
        (
            "2063-12-21",
            None,
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
        ),
        (
            "2063-12-21",
            "2074-07-13",
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
            datetime(2025, 7, 13, 16, 21, 34, tzinfo=timezone.utc),
        ),
    ],
)
def test_cladetime_as_of_dates(sequence_as_of, tree_as_of, expected_sequence_as_of, expected_tree_as_of):
    with freeze_time("2025-07-13 16:21:34"):
        ct = CladeTime(sequence_as_of=sequence_as_of, tree_as_of=tree_as_of)

    assert ct.sequence_as_of == expected_sequence_as_of
    assert ct.tree_as_of == expected_tree_as_of


@pytest.mark.parametrize("bad_date", ["2020-07-13", "2022-12-32"])
def test_cladetime_invalid_date(bad_date):
    with pytest.raises(CladeTimeInvalidDateError):
        CladeTime(sequence_as_of=bad_date, tree_as_of=bad_date)


def test_cladetime_future_date():
    with pytest.warns(CladeTimeFutureDateWarning):
        CladeTime(sequence_as_of="2063-07-13")
    with pytest.warns(CladeTimeFutureDateWarning):
        CladeTime(tree_as_of="2063-07-13")
    with pytest.warns(CladeTimeFutureDateWarning):
        CladeTime(sequence_as_of="2023-12-31", tree_as_of="2063-07-13")


@pytest.mark.parametrize(
    "sequence_as_of, expected_content",
    [
        (
            "2024-09-01",
            "version 4",
        ),
        (
            None,
            "version 4",
        ),
        (
            datetime(2023, 2, 5, 5, 55),
            "version 2",
        ),
        (
            datetime(2023, 2, 5, 1, 22),
            "version 1",
        ),
    ],
)
def test_cladetime_urls(s3_setup, test_config, sequence_as_of, expected_content):
    s3_client, bucket_name, s3_object_keys = s3_setup

    mock = MagicMock(return_value=test_config, name="CladeTime._get_config_mock")

    with patch("cladetime.CladeTime._get_config", mock):
        with freeze_time("2024-09-02 00:00:00"):
            ct = CladeTime(sequence_as_of=sequence_as_of)
            for url in [ct.url_sequence, ct.url_sequence_metadata]:
                parsed_url = urlparse(url)
                key = parsed_url.path.strip("/")
                version_id = parse_qs(parsed_url.query)["versionId"][0]
                object = s3_client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
                assert expected_content in object["Body"].read().decode("utf-8").lower()

            if ct.sequence_as_of < test_config.nextstrain_min_ncov_metadata_date:
                assert ct.url_ncov_metadata is None
            else:
                assert ct.url_ncov_metadata is not None


def test_cladetime_ncov_metadata(s3_setup, s3_object_keys, test_config):
    s3_client, bucket_name, s3_object_keys = s3_setup
    mock = MagicMock(return_value=test_config, name="CladeTime._get_config_mock")
    with patch("cladetime.CladeTime._get_config", mock):
        with freeze_time("2024-09-02 00:00:00"):
            ct = CladeTime()
            version_id = parse_qs(urlparse(ct.url_ncov_metadata).query)["versionId"][0]
            # Generate a presigned URL for the specific version of the object
            presigned_url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": s3_object_keys["ncov_metadata"], "VersionId": version_id},
                ExpiresIn=3600,
            )
            ct.url_ncov_metadata = presigned_url

    assert ct.ncov_metadata.get("nextclade_dataset_version") == "version-4"
    assert ct.ncov_metadata.get("nextclade_dataset_name_full") == "nextstrain/sars-cov-2/wuhan-hu-1/orfs"

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
