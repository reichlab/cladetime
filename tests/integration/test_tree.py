from datetime import datetime, timezone
from urllib.parse import urlparse

import pytest
from freezegun import freeze_time

from cladetime import CladeTime, Tree
from cladetime.exceptions import TreeNotAvailableError
from cladetime.util.reference import _docker_installed

docker_enabled = _docker_installed()


def test__get_tree_url():
    with freeze_time("2024-08-13 16:21:34"):
        ct = CladeTime()
        tree = Tree(ct.tree_as_of, ct.url_sequence)
        tree_url_parts = urlparse(tree.url)
        assert "2024-07-17--12-57-03Z" in tree_url_parts.path
        assert "tree.json" in tree_url_parts.path


def test__get_tree_url_bad_date():
    # we cannot get reference trees prior to 2024-08-01
    ct = CladeTime()
    with pytest.raises(TreeNotAvailableError):
        Tree(datetime(2024, 7, 13, tzinfo=timezone.utc), ct.url_sequence)


def test_tree_ncov_metadata():
    with freeze_time("2024-11-05 16:21:34"):
        # when tree_as_of <> sequence_as_of, the respective ncov_metadata
        # properties of CladeTime and Tree may differ
        ct = CladeTime(sequence_as_of=datetime.now(), tree_as_of="2024-08-02")
        tree = Tree(ct.tree_as_of, ct.url_sequence)
        assert tree.ncov_metadata.get("nextclade_version_num") == "3.8.2"
        assert tree.ncov_metadata.get("nextclade_dataset_version") == "2024-07-17--12-57-03Z"
        assert ct.ncov_metadata.get("nextclade_version_num") == "3.9.1"
        assert ct.ncov_metadata.get("nextclade_dataset_version") == "2024-10-17--16-48-48Z"


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test__get_reference_tree():
    with freeze_time("2024-08-13 16:21:34"):
        ct = CladeTime()
        tree = Tree(ct.tree_as_of, ct.url_sequence)
        assert tree.tree.get("meta", "").get("title", "").lower() == "sars-cov-2 phylogeny"
