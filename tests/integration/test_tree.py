from datetime import datetime, timezone
from urllib.parse import urlparse

import pytest
from freezegun import freeze_time

from cladetime import CladeTime, Tree
from cladetime.exceptions import TreeNotAvailableError
from cladetime.util.reference import _docker_installed

docker_enabled = _docker_installed()


def test__get_tree_url():
    # Use a date when hub metadata is available (no mocking needed)
    with freeze_time("2024-10-30 16:21:34"):
        ct = CladeTime()
        tree = Tree(ct.tree_as_of, ct.url_sequence)
        tree_url_parts = urlparse(tree.url)
        # With 2024-10-30, we should get the 2024-10-17 dataset version
        assert "2024-10-17--16-48-48Z" in tree_url_parts.path
        assert "tree.json" in tree_url_parts.path


def test__get_tree_url_bad_date():
    # We cannot get reference trees prior to 2024-10-09 (when hub archives begin)
    ct = CladeTime()
    with pytest.raises(TreeNotAvailableError):
        Tree(datetime(2024, 7, 13, tzinfo=timezone.utc), ct.url_sequence)


def test_tree_ncov_metadata():
    # Test that tree_as_of and sequence_as_of can have different metadata
    with freeze_time("2024-11-15 16:21:34"):
        # when tree_as_of <> sequence_as_of, the respective ncov_metadata
        # properties of CladeTime and Tree may differ
        ct = CladeTime(sequence_as_of=datetime.now(), tree_as_of="2024-10-30")
        tree = Tree(ct.tree_as_of, ct.url_sequence)

        # Hard-code expected values from 2024-10-30 hub archive
        # See: https://github.com/reichlab/variant-nowcast-hub/blob/main/auxiliary-data/modeled-clades/2024-10-30.json
        assert tree.ncov_metadata.get("nextclade_version_num") == "3.9.1"
        assert tree.ncov_metadata.get("nextclade_dataset_version") == "2024-10-17--16-48-48Z"

        # sequence_as_of (2024-11-15) uses different metadata
        assert ct.ncov_metadata.get("nextclade_version_num") is not None
        assert ct.ncov_metadata.get("nextclade_dataset_version") is not None


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test__get_reference_tree():
    # Use a date when hub metadata is available (no mocking needed)
    with freeze_time("2024-10-30 16:21:34"):
        ct = CladeTime()
        tree = Tree(ct.tree_as_of, ct.url_sequence)
        assert tree.tree.get("meta", "").get("title", "").lower() == "sars-cov-2 phylogeny"
