from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip(
    "cladetime.get_clade_list", reason="get_clade_list moved to variant-nowcast-hub", exc_type=ImportError
)
from cladetime.get_clade_list import main  # noqa


@pytest.fixture
def test_file_path() -> Path:
    """
    Return path to the unit test files.
    """
    test_file_path = Path(__file__).parents[1].joinpath("data")
    return test_file_path


@pytest.mark.parametrize(
    "threshold, weeks, max_clades, expected_list",
    [
        (0.1, 3, 9, ["AA", "AA.ZZ", "BB", "CC", "DD", "EE", "FF"]),
        (0.3, 3, 9, ["AA", "AA.ZZ", "EE"]),
        (0.1, 2, 9, ["AA.ZZ", "AA", "BB", "CC", "DD", "EE", "FF"]),
        (0.1, 1, 9, ["AA", "BB", "CC", "DD", "FF"]),
        (0.3, 1, 9, ["AA"]),
        (0.1, 3, 4, ["AA", "AA.ZZ", "BB", "CC"]),
        (0.3, 3, 2, ["AA", "AA.ZZ"]),
        (0.1, 2, 3, ["AA", "BB", "CC"]),
        (0.1, 1, 3, ["AA", "BB", "CC"]),
        (1, 3, 9, []),
    ],
)
def test_clade_list(test_file_path, tmp_path, threshold, weeks, max_clades, expected_list):
    test_genome_metadata = test_file_path / "test_metadata.tsv"
    mock = MagicMock(return_value=test_genome_metadata, name="genome_metadata_download_mock")

    with patch("cladetime.get_clade_list.download_covid_genome_metadata", mock):
        actual_list = main("some_bucket", "some_key", tmp_path, threshold, weeks, max_clades)

    assert set(expected_list) == set(actual_list)
