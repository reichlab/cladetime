from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from freezegun import freeze_time
from polars.testing import assert_frame_equal

from cladetime import CladeTime, sequence
from cladetime.exceptions import CladeTimeSequenceWarning
from cladetime.util.reference import _docker_installed

docker_enabled = _docker_installed()


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_cladetime_assign_clades(tmp_path, demo_mode):
    # demo_mode fixture overrides CladeTime config to use Nextstrain's 100k sample
    # sequence and sequence metadata instead of the entire universe of SARS-CoV-2 sequences
    # This test uses current (non-historical) data so doesn't need patch_s3_for_tests or freeze_time
    assignment_file = tmp_path / "assignments.tsv"

    ct = CladeTime()

    metadata_filtered = sequence.filter_metadata(ct.sequence_metadata, collection_min_date="2024-10-01")

    # store clade assignments as they exist on the metadata file downloaded from Nextstrain
    original_clade_assignments = metadata_filtered.select(["strain", "clade"])

    # assign clades to the same sequences using cladetime
    assigned_clades = ct.assign_clades(metadata_filtered, output_file=assignment_file)

    # clade assignments via cladetime should match the original clade assignments
    check_clade_assignments = original_clade_assignments.join(
        assigned_clades.detail, on=["strain", "clade"]
    ).collect()
    assert len(check_clade_assignments) == len(metadata_filtered.collect())
    unmatched_clade_count = check_clade_assignments.filter(pl.col("clade").is_null()).shape[0]
    assert unmatched_clade_count == 0

    # summarized clade assignments should also match summarized clade assignments from the
    # original metadata file
    assert_frame_equal(
        sequence.summarize_clades(metadata_filtered.rename({"clade": "clade_nextstrain"})),
        assigned_clades.summary,
        check_column_order=False,
        check_row_order=False,
    )

    # metadata should reflect current ncov metadata
    assert assigned_clades.meta.get("sequence_as_of") is not None
    assert assigned_clades.meta.get("tree_as_of") is not None
    assert assigned_clades.meta.get("nextclade_dataset_version") is not None
    assert assigned_clades.meta.get("nextclade_version_num") is not None
    assert assigned_clades.meta.get("assignment_as_of") is not None


# NOTE: test_cladetime_assign_clades_historical was removed because Nextstrain S3
# no longer retains sequence data before 2025-09-29. The test used freeze_time("2024-10-30")
# which is outside the data availability window. See GitHub issue #185 for details on
# restoring historical test coverage and the limitations imposed by Nextstrain's
# ~7-week data retention policy.


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_cladetime_assign_clades_current_time(tmp_path, demo_mode):
    """
    Test clade assignment works with current (non-frozen) time.

    This test ensures that the metadata pipeline works correctly as of NOW,
    without any time mocking. This catches issues with current S3 data or
    very recent hub archives.
    """
    assignment_file = tmp_path / "assignments_current.tsv"

    # No freeze_time - use actual current time
    ct = CladeTime()

    metadata_filtered = sequence.filter_metadata(
        ct.sequence_metadata,
        collection_min_date="2024-10-01"
    )

    # Assign clades using current reference tree
    assigned_clades = ct.assign_clades(metadata_filtered, output_file=assignment_file)

    # Verify metadata exists (can't hard-code exact values since time is current)
    assert assigned_clades.meta.get("sequence_as_of") is not None
    assert assigned_clades.meta.get("tree_as_of") is not None
    assert assigned_clades.meta.get("nextclade_dataset_version") is not None
    assert assigned_clades.meta.get("nextclade_version_num") is not None
    assert assigned_clades.meta.get("assignment_as_of") is not None

    # Verify assignments were actually made
    assert assigned_clades.meta.get("sequences_to_assign") > 0
    assert assigned_clades.meta.get("sequences_assigned") > 0

    # Verify dataset version is recent (within last 90 days)
    dataset_version_str = assigned_clades.meta.get("nextclade_dataset_version")
    # Format: "2024-10-17--16-48-48Z"
    dataset_date = datetime.strptime(dataset_version_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    current_date = datetime.now(timezone.utc)
    days_old = (current_date - dataset_date).days
    assert days_old <= 90, f"Dataset version is {days_old} days old, may be stale"


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_assign_old_tree(test_file_path, tmp_path, test_sequences, patch_s3_for_tests):
    """Test that different tree_as_of dates can produce different clade assignments.

    This test uses dates within the hub archive range (>= 2024-10-09).
    """
    sequence_file, sequence_set = test_sequences
    sequence_list = list(sequence_set)
    sequence_list.sort()

    fasta_mock = MagicMock(return_value=test_file_path / sequence_file, name="cladetime.sequence.filter")
    test_filtered_metadata = {
        "country": ["USA", "USA", "USA"],
        "date": ["2022-01-02", "2022-01-02", "2023-02-01"],
        "host": ["Homo sapiens", "Homo sapiens", "Homo sapiens"],
        "location": ["Hawaii", "Hawaii", "Utah"],
        "strain": sequence_list,
    }
    metadata_filtered = pl.LazyFrame(test_filtered_metadata)

    # Use dates within hub archive range
    with freeze_time("2024-11-15"):
        current_file = tmp_path / "current_assignments.tsv"
        ct_current_tree = CladeTime()
        with patch("cladetime.sequence.filter", fasta_mock):
            current_assigned_clades = ct_current_tree.assign_clades(metadata_filtered, output_file=current_file)
            current_assigned_clades = current_assigned_clades.detail.select(["strain", "clade"]).collect()

        old_file = tmp_path / "old_assignments.tsv"
        # Use tree_as_of that's within hub range (2024-10-16)
        ct_old_tree = CladeTime(tree_as_of="2024-10-16")
        with patch("cladetime.sequence.filter", fasta_mock):
            old_assigned_clades = ct_old_tree.assign_clades(metadata_filtered, output_file=old_file)
            old_assigned_clade_detail = old_assigned_clades.detail.select(["strain", "clade"]).collect()

    # Verify both assignments processed the same strains
    assert_frame_equal(current_assigned_clades.select("strain"), old_assigned_clade_detail.select("strain"))

    # Check metadata reflects the different dates
    assert old_assigned_clades.meta.get("sequence_as_of") == datetime(2024, 11, 15, tzinfo=timezone.utc)
    assert old_assigned_clades.meta.get("tree_as_of") == datetime(2024, 10, 16, 11, 59, 59, tzinfo=timezone.utc)

    # Verify tree_as_of uses hub metadata
    assert old_assigned_clades.meta.get("nextclade_dataset_version") is not None
    assert old_assigned_clades.meta.get("nextclade_version_num") is not None
    assert old_assigned_clades.meta.get("assignment_as_of") == "2024-11-15 00:00"

@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
@pytest.mark.parametrize("sequence_file", ["test_sequences.fasta.xz", "test_sequences.fasta.zst"])
def test_assign_clade_detail(test_file_path, tmpdir, sequence_file):
    """Test the final clade assignment linefile."""
    test_sequence_file = test_file_path / sequence_file

    # The list below represents sequences in the test fasta file AND test metadata file
    expected_sequence_assignments = {
        "USA/WV064580/2020": "20G"
    }

    mock_download = MagicMock(return_value=test_sequence_file, name="_download_from_url_mock")
    with patch("cladetime.sequence._download_from_url", mock_download):
        ct = CladeTime()
        ct.url_sequence = test_sequence_file.as_uri()  # used to determine extension when reading .fasta file
        test_metadata = pl.read_csv(test_file_path / "metadata.tsv.zst", separator="\t", infer_schema_length=100000) \
            .filter(pl.col("strain").is_in(expected_sequence_assignments.keys())) \
            .lazy()

        clades = ct.assign_clades(test_metadata, output_file=tmpdir / "assignments.tsv")
        detailed_df = clades.detail.collect()

        # assign_clades detail output should have the same number of records as the input metadata
        assert len(detailed_df) == len(expected_sequence_assignments)

        # check actual clade assignments against expected assignments
        strain_clade_dict = detailed_df.select("strain", "clade_nextstrain").to_dicts()
        for item in strain_clade_dict:
            assert expected_sequence_assignments[item["strain"]] == item["clade_nextstrain"]

        # check metadata
        assert clades.meta.get("sequences_to_assign") == 1
        assert clades.meta.get("sequences_assigned") == 1


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
@pytest.mark.parametrize("sequence_file", ["test_sequences.fasta.xz", "test_sequences.fasta.zst"])
def test_assign_clade_detail_missing_assignments(test_file_path, tmpdir, sequence_file):
    """Test the final clade assignment linefile when some sequences are not assigned clades."""
    test_sequence_file = test_file_path / sequence_file

    mock_download = MagicMock(return_value=test_sequence_file, name="_download_from_url_mock")
    with patch("cladetime.sequence._download_from_url", mock_download):
        ct = CladeTime()
        ct.url_sequence = test_sequence_file.as_uri()  # used to determine extension when reading .fasta file
        test_metadata = pl.read_csv(test_file_path / "metadata.tsv.zst", separator="\t", infer_schema_length=100000).lazy()
        test_metadata_count = len(test_metadata.collect())

        clades = ct.assign_clades(test_metadata, output_file=tmpdir / "assignments.tsv")
        detailed_df = clades.detail.collect()

        # assign_clades detail output should have the same number of records as the input metadata
        assert len(detailed_df) == test_metadata_count

        # check metadata
        # only one sequence in the test metadata is in the test fasta
        assert clades.meta.get("sequences_to_assign") == test_metadata_count
        assert clades.meta.get("sequences_assigned") == 1


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
@pytest.mark.parametrize(
    "min_date, max_date, expected_rows",
    [("2023-12-27", None, 1), (None, "2022-01-03", 2), ("2022-01-02", "2023-12-28", 2)],
)
def test_assign_date_filters(test_file_path, tmp_path, test_sequences, min_date, max_date, expected_rows):
    sequence_file, sequence_set = test_sequences
    fasta_mock = MagicMock(return_value=test_file_path / sequence_file, name="cladetime.sequence.filter")
    test_metadata = {
        "date": ["2022-01-01", "2022-01-03", "2023-12-27"],
        "strain": list(sequence_set),
        "clade_nextstrain": ["11C", "11B", "11B"],
        "host": ["Homo sapiens", "Homo sapiens", "Homo sapiens"],
        "country": ["USA", "USA", "USA"],
        "division": ["Utah", "Utah", "Utah"],
        "wombat_count": [2, 22, 222],
    }
    metadata = pl.LazyFrame(test_metadata)
    metadata_filtered = sequence.filter_metadata(
        metadata=metadata, collection_min_date=min_date, collection_max_date=max_date
    )

    ct = CladeTime()
    assignment_file = tmp_path / "assignments.tsv"
    with patch("cladetime.sequence.filter", fasta_mock):
        assigned_clades = ct.assign_clades(metadata_filtered, output_file=assignment_file)
    assert len(assigned_clades.detail.collect()) == expected_rows


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_assign_too_many_sequences_warning(tmp_path, test_file_path, test_sequences):
    sequence_file, sequence_set = test_sequences

    ct = CladeTime()
    ct._config.clade_assignment_warning_threshold = 2
    test_filtered_metadata = {"date": ["2022-01-01", "2022-01-02", "2023-12-27"], "strain": ["aa", "bb", "cc"]}
    metadata_filtered = pl.LazyFrame(test_filtered_metadata)
    fasta_mock = MagicMock(return_value=test_file_path / sequence_file, name="cladetime.sequence.filter")
    with patch("cladetime.sequence.filter", fasta_mock):
        with pytest.warns(CladeTimeSequenceWarning):
            assignments = ct.assign_clades(metadata_filtered, output_file=tmp_path / "assignments.tsv")
            # clade assignment should proceed, despite the warning
            assert len(assignments.detail.collect()) == 3


@pytest.mark.parametrize("empty_input", [(pl.LazyFrame()), (pl.DataFrame()), (pl.DataFrame({"strain": []}))])
def test_assign_clades_no_sequences(empty_input):
    ct = CladeTime()
    with pytest.warns(CladeTimeSequenceWarning):
        assignments = ct.assign_clades(empty_input)
        assert assignments.detail.collect().shape == (0, 0)
        assert assignments.summary.collect().shape == (0, 0)
        assert assignments.meta == {}
