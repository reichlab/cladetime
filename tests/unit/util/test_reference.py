from datetime import datetime, timedelta, timezone

import pytest
import responses

from cladetime.util.reference import _get_metadata_from_hub, _get_s3_object_url


def test__get_s3_object_url(s3_setup):
    s3_client, bucket_name, s3_object_keys = s3_setup

    target_date = datetime.strptime("2023-02-15", "%Y-%m-%d").replace(tzinfo=timezone.utc)
    object_key = s3_object_keys["sequence_metadata_zst"]

    version_id, version_url = _get_s3_object_url(bucket_name, object_key, target_date)

    assert version_id is not None
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key, VersionId=version_id)
    last_modified = s3_object["LastModified"]

    assert s3_object.get("Metadata") == {"version": "3"}
    assert last_modified <= target_date
    assert last_modified == datetime.strptime("2023-02-05 14:33:06", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    assert version_url == f"https://{bucket_name}.s3.amazonaws.com/{object_key}?versionId={version_id}"


# ---- Tests for variant-nowcast-hub fallback mechanism ----


class TestHubFallback:
    """
    Tests for the _get_metadata_from_hub() fallback function.

    This fallback addresses Nextstrain's October 2025 cleanup of historical
    metadata_version.json files by retrieving metadata from variant-nowcast-hub
    archives instead.
    """

    @responses.activate
    def test_get_metadata_from_hub_exact_match(self):
        """Test fallback successfully finds exact date match in Hub archives."""
        date = datetime(2024, 10, 9, tzinfo=timezone.utc)

        # Mock GitHub response with Hub archive for exact date
        responses.add(
            responses.GET,
            "https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/2024-10-09.json",
            json={
                "clades": ["24A", "24B", "24C"],
                "meta": {
                    "created_at": "2024-10-07T03:12:13+00:00",
                    "ncov": {
                        "nextclade_dataset_name_full": "nextstrain/sars-cov-2/wuhan-hu-1/orfs",
                        "nextclade_dataset_version": "2024-09-25--21-50-30Z",
                        "nextclade_version": "nextclade 3.8.2",
                        "nextclade_version_num": "3.8.2"
                    }
                }
            },
            status=200
        )

        metadata = _get_metadata_from_hub(date)

        # Verify all required metadata fields are present and correct
        assert metadata["nextclade_dataset_name_full"] == "nextstrain/sars-cov-2/wuhan-hu-1/orfs"
        assert metadata["nextclade_dataset_version"] == "2024-09-25--21-50-30Z"
        assert metadata["nextclade_version"] == "nextclade 3.8.2"
        assert metadata["nextclade_version_num"] == "3.8.2"

    @responses.activate
    def test_get_metadata_from_hub_nearest_prior(self):
        """Test fallback finds nearest prior archive when exact match missing."""
        # Use a date that isn't a Wednesday (when archives are created)
        date = datetime(2024, 10, 12, tzinfo=timezone.utc)  # Saturday

        # Mock 404 for exact date
        responses.add(
            responses.GET,
            "https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/2024-10-12.json",
            status=404
        )

        # Mock 404 for days 1-2 back
        for days_back in range(1, 3):
            prior_date = date - timedelta(days=days_back)
            date_str = prior_date.strftime("%Y-%m-%d")
            responses.add(
                responses.GET,
                f"https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/{date_str}.json",
                status=404
            )

        # Mock success for 3 days back (2024-10-09, which is a Wednesday)
        responses.add(
            responses.GET,
            "https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/2024-10-09.json",
            json={
                "clades": ["24A", "24B"],
                "meta": {
                    "created_at": "2024-10-07T03:12:13+00:00",
                    "ncov": {
                        "nextclade_dataset_name_full": "nextstrain/sars-cov-2/wuhan-hu-1/orfs",
                        "nextclade_dataset_version": "2024-09-25--21-50-30Z",
                        "nextclade_version_num": "3.8.2"
                    }
                }
            },
            status=200
        )

        metadata = _get_metadata_from_hub(date)

        # Verify metadata from nearest prior archive is returned
        assert metadata is not None
        assert metadata["nextclade_dataset_version"] == "2024-09-25--21-50-30Z"
        assert metadata["nextclade_version_num"] == "3.8.2"

    @responses.activate
    def test_get_metadata_from_hub_no_archive_found(self):
        """Test fallback raises ValueError when no archive found within 30 days."""
        # Use a date after hub archives began but where no archive exists in 30-day window
        date = datetime(2024, 11, 15, tzinfo=timezone.utc)

        # Mock 404 for all dates in 30-day window
        for days_back in range(31):
            prior_date = date - timedelta(days=days_back)
            date_str = prior_date.strftime("%Y-%m-%d")
            responses.add(
                responses.GET,
                f"https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/{date_str}.json",
                status=404
            )

        # Verify ValueError is raised with appropriate message
        with pytest.raises(ValueError, match="No variant-nowcast-hub archive found"):
            _get_metadata_from_hub(date)

    def test_get_metadata_from_hub_date_too_early(self):
        """Test fallback raises ValueError when date is before hub archives begin (2024-10-09)."""
        # Use a date before Hub archives began
        date = datetime(2024, 7, 1, tzinfo=timezone.utc)

        # Verify ValueError is raised with appropriate message
        with pytest.raises(ValueError, match="Hub metadata archives only available from 2024-10-09"):
            _get_metadata_from_hub(date)

    @responses.activate
    def test_get_metadata_from_hub_malformed_response(self):
        """Test fallback handles malformed JSON response gracefully."""
        date = datetime(2024, 10, 9, tzinfo=timezone.utc)

        # Mock response with malformed JSON (missing meta.ncov)
        responses.add(
            responses.GET,
            "https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/2024-10-09.json",
            json={
                "clades": ["24A", "24B"],
                "meta": {}  # Missing "ncov" key
            },
            status=200
        )

        # This should raise KeyError when trying to access meta["ncov"]
        with pytest.raises(KeyError):
            _get_metadata_from_hub(date)

    @responses.activate
    def test_get_metadata_from_hub_within_30_day_window(self):
        """Test fallback searches up to 30 days back."""
        date = datetime(2024, 10, 31, tzinfo=timezone.utc)

        # Mock 404 for first 29 days
        for days_back in range(30):
            prior_date = date - timedelta(days=days_back)
            date_str = prior_date.strftime("%Y-%m-%d")
            responses.add(
                responses.GET,
                f"https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/{date_str}.json",
                status=404
            )

        # Mock success for exactly 30 days back (edge of window)
        responses.add(
            responses.GET,
            "https://raw.githubusercontent.com/reichlab/variant-nowcast-hub/main/auxiliary-data/modeled-clades/2024-10-01.json",
            json={
                "clades": ["24A"],
                "meta": {
                    "ncov": {
                        "nextclade_dataset_name_full": "nextstrain/sars-cov-2/wuhan-hu-1/orfs",
                        "nextclade_dataset_version": "2024-09-20--12-00-00Z",
                        "nextclade_version_num": "3.8.0"
                    }
                }
            },
            status=200
        )

        metadata = _get_metadata_from_hub(date)

        # Verify metadata from 30 days back is successfully retrieved
        assert metadata["nextclade_dataset_version"] == "2024-09-20--12-00-00Z"
