import lzma
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests
from freezegun import freeze_time
from polars.testing import assert_frame_equal, assert_frame_not_equal

from cladetime import CladeTime, sequence
from cladetime.exceptions import CladeTimeSequenceWarning
from cladetime.util.config import Config
from cladetime.util.reference import _docker_installed, _get_s3_object_url

docker_enabled = _docker_installed()


@pytest.fixture()
def metadata_100k(tmp_path) -> pl.LazyFrame:
    "Return metadata for Nextstain's 100k samples as of 2024-11-01"
    config = Config()
    metadata_url = _get_s3_object_url(
        bucket_name=config.nextstrain_ncov_bucket,
        object_key="files/ncov/open/100k/metadata.tsv.xz",
        date=datetime(2024, 11, 1, tzinfo=timezone.utc),
    )[1]

    # download test metadata for Nextstrain's 100k samples (we can't use polars to scan it from
    # s3 like we usually do, because the 100k samples don't have ZSTD-compressed versions
    # and lmza.open doesn't support https)
    response = requests.get(metadata_url)
    response.raise_for_status()
    with open(tmp_path / "metadata.tsv.xz", "wb") as file:
        file.write(response.content)
    metadata = pl.read_csv(lzma.open(tmp_path / "metadata.tsv.xz"), separator="\t", infer_schema_length=100000).lazy()

    return metadata


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_cladetime_assign_clades(tmp_path, metadata_100k):
    config = Config()
    assignment_file = tmp_path / "assignments.csv"

    with freeze_time("2024-11-01"):
        ct = CladeTime()

        # override link to sequence .fasta to test against the 100k sample dataset
        sequence_url = _get_s3_object_url(
            bucket_name=config.nextstrain_ncov_bucket,
            object_key="files/ncov/open/100k/sequences.fasta.xz",
            date=datetime(2024, 11, 1, tzinfo=timezone.utc),
        )[1]
        ct.url_sequence = sequence_url

        metadata_filtered = sequence.filter_metadata(metadata_100k, collection_min_date="2024-10-01")

        # store clade assignments as they exist on the metadata file downloaded from Nextstrain
        original_clade_assignments = metadata_filtered.select(["strain", "clade"])

        # assign clades to the same sequences using cladetime
        assigned_clades = ct.assign_clades(metadata_filtered, output_file=assignment_file)

        # clade assignments via cladetime should match the original clade assignments
        check_clade_assignments = original_clade_assignments.join(assigned_clades, on=["strain", "clade"]).collect()
        assert len(check_clade_assignments) == len(metadata_filtered.collect())
        unmatched_clade_count = check_clade_assignments.filter(pl.col("clade").is_null()).shape[0]
        assert unmatched_clade_count == 0


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_assign_old_tree(test_file_path, tmp_path, test_sequences):
    sequence_file, sequence_set = test_sequences

    fasta_mock = MagicMock(return_value=test_file_path / sequence_file, name="cladetime.sequence.filter")
    test_filtered_metadata = {"date": ["2022-01-01", "2022-01-02", "2023-12-27"], "strain": list(sequence_set)}
    metadata_filtered = pl.LazyFrame(test_filtered_metadata)

    # expected clade assignments for 2024-08-02 (as retrieved from Nextrain metadata)
    expected_assignment_dict = {
        "strain": ["USA/VA-CDC-LC1109961/2024", "USA/FL-CDC-LC1109983/2024", "USA/MD-CDC-LC1110088/2024"],
        "clade": ["24C", "24B", "24B"],
    }
    expected_assignments = pl.DataFrame(expected_assignment_dict)

    with freeze_time("2024-11-01"):
        current_file = tmp_path / "current_assignments.csv"
        ct_current_tree = CladeTime()
        with patch("cladetime.sequence.filter", fasta_mock):
            current_assigned_clades = ct_current_tree.assign_clades(metadata_filtered, output_file=current_file)
            current_assigned_clades = current_assigned_clades.select(["strain", "clade"]).collect()

        old_file = tmp_path / "old_assignments.csv"
        ct_old_tree = CladeTime(tree_as_of="2024-08-02")
        with patch("cladetime.sequence.filter", fasta_mock):
            old_assigned_clades = ct_old_tree.assign_clades(metadata_filtered, output_file=old_file)
            old_assigned_clades = old_assigned_clades.select(["strain", "clade"]).collect()

    assert_frame_equal(current_assigned_clades.select("strain"), old_assigned_clades.select("strain"))
    assert_frame_not_equal(current_assigned_clades.select("clade"), old_assigned_clades.select("clade"))
    assert_frame_equal(old_assigned_clades.sort("strain"), expected_assignments.sort("strain"))


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
    assignment_file = tmp_path / "assignments.csv"
    with patch("cladetime.sequence.filter", fasta_mock):
        assigned_clades = ct.assign_clades(metadata_filtered, output_file=assignment_file)
    assert len(assigned_clades.collect()) == expected_rows


def test_assign_too_many_sequences_warning(tmp_path, test_file_path, test_sequences):
    sequence_file, sequence_set = test_sequences

    ct = CladeTime()
    ct._config.clade_assignment_warning_threshold = 2
    test_filtered_metadata = {"date": ["2022-01-01", "2022-01-02", "2023-12-27"], "strain": ["aa", "bb", "cc"]}
    metadata_filtered = pl.LazyFrame(test_filtered_metadata)
    fasta_mock = MagicMock(return_value=test_file_path / sequence_file, name="cladetime.sequence.filter")
    with patch("cladetime.sequence.filter", fasta_mock):
        with pytest.warns(CladeTimeSequenceWarning):
            assignments = ct.assign_clades(metadata_filtered, output_file=tmp_path / "assignments.csv")
            # clade assignment should proceed, despite the warning
            assert len(assignments.collect()) == 3


def test_assign_clades_no_sequences():
    ct = CladeTime()
    with pytest.warns(CladeTimeSequenceWarning):
        assignments = ct.assign_clades(
            pl.LazyFrame(),
        )
        assert assignments.collect().shape == (0, 0)
