from urllib.parse import urlparse

import pytest
from cladetime import CladeTime, Tree
from cladetime.exceptions import TreeNotAvailableError
from cladetime.util.reference import _docker_installed
from freezegun import freeze_time

docker_enabled = _docker_installed()


def test__get_tree_url():
    with freeze_time("2024-08-13 16:21:34"):
        tree = Tree(CladeTime())
        tree_url_parts = urlparse(tree.url)
        assert "2024-07-17--12-57-03Z" in tree_url_parts.path
        assert "tree.json" in tree_url_parts.path


def test__get_tree_url_bad_date():
    # we cannot get reference trees prior to 2024-08-01
    with pytest.raises(TreeNotAvailableError):
        Tree(CladeTime(tree_as_of="2024-07-13"))


@pytest.mark.skipif(not docker_enabled, reason="Docker is not installed")
def test__get_reference_tree():
    with freeze_time("2024-08-13 16:21:34"):
        tree = Tree(CladeTime())
        assert tree.tree.get("meta", "").get("title", "").lower() == "sars-cov-2 phylogeny"
