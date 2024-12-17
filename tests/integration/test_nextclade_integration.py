import zipfile

import polars as pl
import pytest

from cladetime.util.reference import _docker_installed, _get_clade_assignments, _get_nextclade_dataset

docker_enabled = _docker_installed()


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_get_nextclade_dataset(tmp_path):
    dataset_path = _get_nextclade_dataset("latest", "sars-cov-2", "2024-07-17--12-57-03Z", tmp_path)

    assert "2024-07-17--12-57-03Z" in str(dataset_path)

    # Nextclade dataset contains files needed as inputs for clade assignment
    zip = zipfile.ZipFile(str(dataset_path))
    assert {"reference.fasta", "tree.json"}.issubset(set(zip.namelist()))


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_get_clade_assignments(test_file_path, tmp_path):
    test_sequence_set = {
        "USA/MD-MDH-1820/2021",
        "USA/CA-CDPH-A3000000297958/2023",
        "USA/WV064580/2020",
        "USA/PA-CDC-LC1096774/2024",
        "USA/NJ-CDC-LC1124615/2024",
    }

    sequence_file = test_file_path / "test_sequences.fasta.xz"
    nextclade_dataset = test_file_path / "test_nextclade_dataset.zip"
    # _get_clade_assignments should create the output directory if it doesn't exist
    output_file = tmp_path / "clade_assignments" / "nextclade_assignments.tsv"

    assignment_file = _get_clade_assignments("latest", sequence_file, nextclade_dataset, output_file)
    assignment_df = pl.read_csv(assignment_file, separator="\t").select(
        ["seqName", "clade", "clade_nextstrain", "Nextclade_pango"]
    )

    assert len(assignment_df) == 5
    assigned_sequence_set = set(assignment_df["seqName"].unique().to_list())
    assert test_sequence_set == assigned_sequence_set
    assert assignment_df["clade"].is_null().any() is False


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_get_clade_assignments_no_matches(test_file_path, tmp_path):
    sequence_file = test_file_path / "test_sequences_fake.fasta"
    nextclade_dataset = test_file_path / "test_nextclade_dataset.zip"
    # _get_clade_assignments should create the output directory if it doesn't exist
    output_file = tmp_path / "clade_assignments" / "nextclade_assignments.tsv"

    assignment_file = _get_clade_assignments("latest", sequence_file, nextclade_dataset, output_file)
    assignment_df = pl.read_csv(assignment_file, separator="\t").select(
        ["seqName", "clade", "clade_nextstrain", "Nextclade_pango"]
    )

    assert len(assignment_df) == 4
    assert assignment_df["clade"].is_null().all()
