from unittest.mock import MagicMock, patch

import pytest
from cladetime import CladeTime, Tree


@pytest.mark.skip("This is harder than it needs to be: refactor?")
def test_tree_url(s3_setup, test_config):
    s3_client, bucket_name, s3_object_keys = s3_setup

    # ncov_metadata_presigned_url = s3_client.generate_presigned_url(
    #     "get_object",
    #     Params={"Bucket": bucket_name, "Key": "data/object-key/metadata_version.json"},
    #     ExpiresIn=3600,
    # )

    mock_ncov_metadata = {
        "nextclade_dataset_version": "version-4",
        "nextclade_dataset_name": "sars-cov-2",
        "nextclade_dataset_name_full": "data/clades",
    }

    mock = MagicMock(return_value=test_config, name="CladeTime._get_config_mock")

    with patch("cladetime.CladeTime._get_config", mock):
        ct = CladeTime(tree_as_of="2024-09-02")

    metadata_mock = MagicMock(return_value=mock_ncov_metadata, name="CladeTime.util.tree.Tree_get_ncov_metadata")
    with patch("cladetime.util.tree.Tree._get_ncov_metadata", metadata_mock):
        tree = Tree(ct)

    assert tree == tree
