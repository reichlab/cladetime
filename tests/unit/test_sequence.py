import lzma
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from Bio import SeqIO
from polars.testing import assert_frame_equal

from cladetime import sequence
from cladetime.types import StateFormat


@pytest.fixture
def df_assignments():
    return pl.DataFrame(
        {
            "seqName": [
                "PP782799.1 Severe acute respiratory syndrome coronavirus 2 isolate SARS-CoV-2/human/USA/NY-PV74597/2022",
                "ABCDEFG Severe caffeine deprivation virus",
                "12345678 ",
            ],
            "clade": ["BA.5.2.1", "XX.99.88.77", "howdy"],
        }
    )


@pytest.fixture
def test_file_path() -> Path:
    """
    Return path to the unit test files.
    """
    test_file_path = Path(__file__).parents[1].joinpath("data")
    return test_file_path


@pytest.mark.parametrize("metadata_file", ["metadata.tsv.zst", "metadata.tsv.xz"])
def test_get_metadata(test_file_path, metadata_file):
    metadata_path = test_file_path / metadata_file

    metadata = sequence.get_metadata(metadata_path)
    metadata_cols = set(metadata.collect_schema().names())

    expected_cols = {
        "date",
        "host",
        "country",
        "division",
        "clade_nextstrain",
        "strain",
    }
    assert expected_cols.issubset(metadata_cols)


def test_get_metadata_url(s3_setup):
    """
    Test get_metadata when used with an S3 URL instead of a local file.
    Needs additional research into moto and S3 url access.
    """
    s3_client, bucket_name, s3_object_keys = s3_setup

    # get metadata file from S3 using ZSTD compression
    presigned_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": s3_object_keys["sequence_metadata"]},
        ExpiresIn=3600,
    )
    metadata = sequence.get_metadata(metadata_url=presigned_url)
    assert isinstance(metadata, pl.LazyFrame)

    # get metadata file from S3 using XZ compression
    presigned_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": s3_object_keys["sequence_metadata_xz"]},
        ExpiresIn=3600,
    )
    metadata = sequence.get_metadata(metadata_url=presigned_url)
    assert isinstance(metadata, pl.LazyFrame)


def test_filter_metadata():
    test_genome_metadata = {
        "date": ["2022-01-01", "2022-01-02", "2022-01-03", "2023-12-25", None, "2023-12-27", "2023-05"],
        "host": [
            "Homo sapiens",
            "Homo sapiens",
            "Homo sapiens",
            "Narwhals",
            "Homo sapiens",
            "Homo sapiens",
            "Homo sapiens",
        ],
        "country": ["USA", "Argentina", "USA", "USA", "USA", "USA", "USA"],
        "division": ["Alaska", "Maine", "Guam", "Puerto Rico", "Utah", "Washington DC", "Pennsylvania"],
        "clade_nextstrain": ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "FFF"],
        "location": ["Vulcan", "Reisa", "Bajor", "Deep Space 9", "Earth", "Cardassia", "Cardassia"],
        "strain": ["A1", "A2", "B1", "B2", "C1", "C2", "C2"],
        "unwanted_column": [1, 2, 3, 4, 5, 6, 7],
    }

    lf_metadata = pl.LazyFrame(test_genome_metadata)
    lf_filtered = sequence.filter_metadata(lf_metadata).collect()

    assert len(lf_filtered) == 2

    locations = lf_filtered["location"].to_list()
    locations.sort()
    assert locations == ["AK", "DC"]

    actual_schema = lf_filtered.collect_schema()
    expected_schema = pl.Schema(
        {
            "clade": pl.String,
            "country": pl.String,
            "date": pl.Date,
            "strain": pl.String,
            "host": pl.String,
            "location": pl.String,
        }
    )
    assert actual_schema == expected_schema


@pytest.mark.parametrize(
    "min_date, max_date, expected_rows",
    [
        (datetime(2023, 1, 1), None, 2),
        (None, datetime(2023, 1, 1), 2),
        (datetime(2022, 1, 3), datetime(2023, 12, 25), 2),
    ],
)
def test_filter_metadata_dates(min_date, max_date, expected_rows):
    num_test_rows = 7
    test_genome_metadata = {
        "date": ["2022-01-01", "2022-01-02", "2022-01-03", "2023-12-25", None, "2023-12-27", "2023-05"],
        "host": ["Homo sapiens"] * num_test_rows,
        "country": ["USA", "Argentina", "USA", "USA", "USA", "USA", "USA"],
        "division": ["Massachusetts"] * num_test_rows,
        "clade_nextstrain": ["AAA"] * num_test_rows,
        "location": ["Earth"] * num_test_rows,
        "strain": ["A1"] * num_test_rows,
    }

    lf_metadata = pl.LazyFrame(test_genome_metadata)
    lf_filtered = sequence.filter_metadata(
        lf_metadata, collection_min_date=min_date, collection_max_date=max_date
    ).collect()

    assert len(lf_filtered) == expected_rows


def test_filter_metadata_state_name():
    num_test_rows = 4
    test_genome_metadata = {
        "date": ["2022-01-01"] * num_test_rows,
        "host": ["Homo sapiens"] * num_test_rows,
        "country": ["USA"] * num_test_rows,
        "clade_nextstrain": ["AAA"] * num_test_rows,
        "location": ["Earth"] * num_test_rows,
        "strain": ["A1"] * num_test_rows,
        "division": ["Alaska", "Puerto Rico", "Washington DC", "Fake State"],
    }

    lf_metadata = pl.LazyFrame(test_genome_metadata)
    lf_filtered = sequence.filter_metadata(lf_metadata, state_format=StateFormat.NAME)
    lf_filtered = lf_filtered.collect()

    # Un-mapped states are dropped from dataset
    assert len(lf_filtered) == 3

    locations = set(lf_filtered["location"].to_list())
    assert locations == {"Alaska", "Puerto Rico", "Washington DC"}


def test_filter_metadata_state_fips():
    num_test_rows = 4
    test_genome_metadata = {
        "date": ["2022-01-01"] * num_test_rows,
        "host": ["Homo sapiens"] * num_test_rows,
        "country": ["USA"] * num_test_rows,
        "clade_nextstrain": ["AAA"] * num_test_rows,
        "location": ["Earth"] * num_test_rows,
        "strain": ["A1"] * num_test_rows,
        "division": ["Massachusetts", "Puerto Rico", "Washington DC", "Fake State"],
    }

    lf_metadata = pl.LazyFrame(test_genome_metadata)
    lf_filtered = sequence.filter_metadata(lf_metadata, state_format=StateFormat.FIPS)
    lf_filtered = lf_filtered.collect()

    # Un-mapped states are dropped from dataset
    assert len(lf_filtered) == 3

    locations = set(lf_filtered["location"].to_list())
    assert locations == {"11", "25", "72"}


def test_get_metadata_ids():
    metadata = {
        "strain": ["A1", "A2", "A2", "A4"],
        "country": ["USA", "Canada", "Mexico", "Brazil"],
        "location": ["Earth", "Earth", "Earth", "Earth"],
    }
    expected_set = {"A1", "A2", "A4"}

    lf = pl.LazyFrame(metadata)
    seq_set = sequence.get_metadata_ids(lf)
    assert seq_set == expected_set

    df = lf.collect()
    seq_set = sequence.get_metadata_ids(df)
    assert seq_set == expected_set

    empty_lf = pl.LazyFrame([])
    with pytest.raises(ValueError):
        seq_set = sequence.get_metadata_ids(empty_lf)


def test_filter(test_file_path, tmpdir):
    test_sequence_file = test_file_path / "test_sequence.xz"
    test_sequence_set = {
        "USA/MD-MDH-1820/2021",
        "USA/CA-CDPH-A3000000297958/2023",
        "USA/WV064580/2020",
        "USA/PA-CDC-LC1096774/2024",
        "STARFLEET/DS9-DS9-001/2024",
    }
    mock_download = MagicMock(return_value=test_sequence_file, name="_download_from_url_mock")
    with patch("cladetime.sequence._download_from_url", mock_download):
        filtered_sequence_file = sequence.filter(test_sequence_set, "http://thisismocked.com", tmpdir)

    test_sequence_set.remove("STARFLEET/DS9-DS9-001/2024")
    actual_headers = []
    with open(filtered_sequence_file, "r") as fasta_test:
        for record in SeqIO.parse(fasta_test, "fasta"):
            actual_headers.append(record.description)
    assert set(actual_headers) == test_sequence_set


def test_filter_no_sequences(test_file_path, tmpdir):
    """Test filter with empty sequence set."""
    test_sequence_file = test_file_path / "test_sequence.xz"
    test_sequence_set = {}
    mock_download = MagicMock(return_value=test_sequence_file, name="_download_from_url_mock")
    with patch("cladetime.sequence._download_from_url", mock_download):
        filtered_no_sequence = sequence.filter(test_sequence_set, "http://thisismocked.com", tmpdir)

    contents = filtered_no_sequence.read_text(encoding=None)
    assert len(contents) == 0


def test_filter_empty_fasta(tmpdir):
    # sequence file is empty
    test_sequence_set = {"A", "B", "C", "D"}
    empty_sequence_file = tmpdir / "empty_sequence_file.xz"
    with lzma.open(empty_sequence_file, "wb"):
        pass
    mock_download = MagicMock(return_value=empty_sequence_file, name="_download_from_url_mock")
    with patch("cladetime.sequence._download_from_url", mock_download):
        seq_filtered = sequence.filter(test_sequence_set, "http://thisismocked.com", tmpdir)
    contents = seq_filtered.read_text(encoding=None)
    assert len(contents) == 0


def test_summarize_clades():
    test_metadata = pl.DataFrame(
        {
            "clade_nextstrain": ["11C", "11C", "11C"],
            "country": ["USA", "USA", "USA"],
            "date": ["2022-01-01", "2022-01-01", "2023-12-27"],
            "host": ["Homo sapiens", "Homo sapiens", "Homo sapiens"],
            "location": ["Utah", "Utah", "Utah"],
            "strain": ["abc/123", "abc/456", "def/123"],
            "wombat_count": [2, 22, 222],
        }
    )

    expected_summary = pl.DataFrame(
        {
            "clade_nextstrain": ["11C", "11C"],
            "country": ["USA", "USA"],
            "date": ["2022-01-01", "2023-12-27"],
            "host": ["Homo sapiens", "Homo sapiens"],
            "location": ["Utah", "Utah"],
            "count": [2, 1],
        }
    ).cast({"count": pl.UInt32})

    summarized = sequence.summarize_clades(test_metadata)
    assert_frame_equal(expected_summary, summarized, check_column_order=False, check_row_order=False)


def test_summarize_clades_custom_group():
    test_metadata = pl.LazyFrame(
        {
            "clade_nextstrain": ["11C", "11C", "11C"],
            "country": ["Canada", "USA", "USA"],
            "date": ["2022-01-01", "2022-01-01", "2023-12-27"],
            "host": ["Homo sapiens", "Homo sapiens", "Homo sapiens"],
            "location": ["Utah", "Utah", "Utah"],
            "strain": ["abc/123", "abc/456", "def/123"],
            "wombat_count": [2, 22, 22],
        }
    )

    expected_summary = pl.LazyFrame(
        {
            "country": ["Canada", "USA"],
            "wombat_count": [2, 22],
            "count": [1, 2],
        }
    ).cast({"count": pl.UInt32})

    summarized = sequence.summarize_clades(test_metadata, group_by=["country", "wombat_count"])
    assert_frame_equal(expected_summary, summarized, check_column_order=False, check_row_order=False)

    test_metadata = pl.LazyFrame(
        {
            "clade_nextstrain": ["11C", "11C", "11C"],
            "country": ["Canada", "USA", "USA"],
            "date": ["2022-01-01", "2022-01-01", "2023-12-27"],
        }
    )

    expected_summary = pl.LazyFrame(
        {
            "clade_nextstrain": ["11C"],
            "count": [3],
        }
    ).cast({"count": pl.UInt32})

    summarized = sequence.summarize_clades(test_metadata, group_by=["clade_nextstrain"])
    assert_frame_equal(expected_summary, summarized, check_column_order=False, check_row_order=False)
