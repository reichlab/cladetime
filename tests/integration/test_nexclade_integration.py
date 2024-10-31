import zipfile

import pytest
from cladetime.util.reference import _docker_installed, get_nextclade_dataset

docker_enabled = _docker_installed()


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test_get_nextclade_dataset(tmpdir):
    dataset_path = get_nextclade_dataset("latest", "sars-cov-2", "2024-07-17--12-57-03Z", tmpdir)

    assert "2024-07-17--12-57-03Z" in str(dataset_path)

    # Nextclade dataset contains files needed as inputs for clade assignment
    zip = zipfile.ZipFile(str(dataset_path))
    assert {"reference.fasta", "tree.json"}.issubset(set(zip.namelist()))
