"""Class to handle Nexstrain-generated SARS-CoV-2 phylogenetic trees."""

import json
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import structlog

from cladetime import sequence
from cladetime.exceptions import NextcladeNotAvailableError, TreeNotAvailableError
from cladetime.util.config import Config
from cladetime.util.reference import (
    _docker_installed,
    _get_nextclade_dataset,
    _get_s3_object_url,
)
from cladetime.util.sequence import _get_ncov_metadata

logger = structlog.get_logger()


class Tree:
    """Interface for current and historic SARS-CoV-2 reference trees.

    The Tree class represents a SARS-CoV-2 phylogenetic reference tree
    in Auspice JSON v2 format. Tree is instantiated with an optional
    argument that specifies the point in time at which to access the
    SARS-CoV-2 reference tree generated by Nextstrain.

    Parameters
    ----------
    clade_time : cladetime.CladeTime
        A CladeTime instance that will be used to set the version of the
        Nextstrain reference tree represented by this Tree instance.
    """

    def __init__(self, tree_as_of: datetime, url_sequence: str):
        """Tree constructor."""
        self._config = Config()
        self.as_of = tree_as_of
        self.url_sequence = url_sequence
        self._nextclade_data_url = self._config.nextclade_data_url
        self._nextclade_data_url_version = self._config.nextclade_data_url_version
        self._tree_name = self._config.nextclade_input_tree_name

        # Nextstrain began publishing ncov pipeline metadata starting on 2024-08-01
        min_tree_date = self._config.nextstrain_min_ncov_metadata_date
        if self.as_of >= min_tree_date:
            self.url_ncov_metadata = _get_s3_object_url(
                self._config.nextstrain_ncov_bucket, self._config.nextstrain_ncov_metadata_key, self.as_of
            )[1]
        else:
            raise TreeNotAvailableError(f"References tree not available for dates prior to {min_tree_date}")
        self._ncov_metadata = self.ncov_metadata
        self._url = self.url

    def __repr__(self):
        cls = self.__class__.__name__
        return f"{cls}(as_of={self.as_of.strftime('%Y-%m-%d')})"

    def __str__(self):
        return f"Represents Nexclade reference tree data as of {self.as_of.strftime('%Y-%m-%d')}"

    @property
    def ncov_metadata(self) -> dict:
        """
        dict : Metadata from the Nextstrain pipeline run that corresponds
        to as_of.
        """
        if self.url_ncov_metadata:
            metadata = sequence._get_ncov_metadata(self.url_ncov_metadata)
            return metadata
        else:
            metadata = {}
        return metadata

    @property
    def url(self) -> str:
        """
        str : URL to the JSON file that represents the SARS-CoV-2
        reference tree that was current for the class's as_of value.

        .. warning::
            This property is an experimental convenience that allows quick
            browsing of a reference tree. Use the :py:attr:`tree<tree>`
            property for programmatic access to a reference tree.
        """
        try:
            return self._get_tree_url()
        except TreeNotAvailableError as err:
            raise err

    @property
    def tree(self) -> dict:
        """
        dict : A SARS-CoV-2 reference tree in `Nextstrain Auspice JSON format
        <https://docs.nextstrain.org/projects/auspice/en/stable/releases/v2.html#new-dataset-json-format>`_.
        """
        if _docker_installed():
            try:
                return self._get_reference_tree()
            except (NextcladeNotAvailableError, TreeNotAvailableError) as err:
                raise err
        else:
            return {}

    def _get_tree_url(self):
        """Get the URL to a Nextclade SARS-CoV-2 reference tree.

        This function retrieves ncov metadata that corresponds to the
        tree_as_of date specified in the class's clade_time parameter.
        The metadata contains information about the Nextclade dataset
        it used for clade assignment. _get_tree_url constructs a URL to
        the Nextclade dataset's tree.json file and returns it.

        Returns:
        --------
        str
            URL to the tree.json file used for clade assignment as of
            the date specified in :any:`cladetime.Tree.as_of<as_of>`.

        Raises:
        -------
        TreeNotAvailableError
            If there is no ncov metadata available for the specified date.
        """

        # we can only reliably retrieve the a past reference tree if we
        # have access to the ncov metadata for that date
        min_tree_as_of = self._config.nextstrain_min_ncov_metadata_date
        if min_tree_as_of > self.as_of:
            logger.error("Reference tree not available", tree_as_of=self.as_of)
            raise TreeNotAvailableError(
                f"Reference tree not available for {self.as_of} (earliest available tree date is {min_tree_as_of})"
            )

        # get the ncov metadata as of the CladeTime's tree_as_of date
        url_ncov_metadata = self.url_ncov_metadata

        if url_ncov_metadata is None:
            logger.error("Reference tree not available", tree_as_of=self.clade_time.tree_as_of)
            raise TreeNotAvailableError(f"Reference tree not available for {self.clade_time.tree_as_of}")

        ncov_metadata = _get_ncov_metadata(url_ncov_metadata)
        nextclade_dataset_name = ncov_metadata.get("nextclade_dataset_name_full")
        nextclade_dataset_version = ncov_metadata.get("nextclade_dataset_version")

        # nextclade_data_url = "https://data.clades.nextstrain.org/v3/"
        tree_url = urljoin(
            self._nextclade_data_url,
            f"{self._nextclade_data_url_version}/{nextclade_dataset_name}/{nextclade_dataset_version}/{self._tree_name}",
        )
        return tree_url

    def _get_reference_tree(self) -> dict:
        """Return a reference tree used for SARS-CoV-2 clade assignments

        Retrieves the reference tree that was current as of
        :any:`tree_as_of<tree_as_of>`. The reference tree is expressed in
        `Nextstrain Auspice JSON format
        <https://docs.nextstrain.org/projects/auspice/en/stable/releases/v2.html#new-dataset-json-format>`_.

        Returns
        -------
        dict
            A Python dictionary that represents the reference tree.
        """
        # get the ncov metadata as of the CladeTime's tree_as_of date
        if self.url_ncov_metadata is None:
            logger.error("Reference tree not available", tree_as_of=self.as_of)
            raise TreeNotAvailableError(f"Reference tree not available for {self.as_of}")

        nextclade_version_num = self.ncov_metadata.get("nextclade_version_num", "")
        nextclade_dataset_name = self.ncov_metadata.get("nextclade_dataset_name", "")
        nextclade_dataset_version = self.ncov_metadata.get("nextclade_dataset_version", "")
        if not all([nextclade_version_num, nextclade_dataset_name, nextclade_dataset_version]):
            logger.error("Incomplete ncov metadata", tree_as_of=self._clade_time.tree_as_of)
            raise TreeNotAvailableError(f"Incomplete ncov metadata {self.ncov_metadata}")

        with tempfile.TemporaryDirectory() as tmpdir:
            nextclade_dataset = _get_nextclade_dataset(
                nextclade_version_num, nextclade_dataset_name.lower(), nextclade_dataset_version, Path(tmpdir)
            )
            zip = zipfile.ZipFile(str(nextclade_dataset))
            with zip.open("tree.json") as tree_file:
                tree = json.loads(tree_file.read())

        return tree
