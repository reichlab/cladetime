"""Class for clade time traveling."""

import warnings
from datetime import datetime, timezone

import polars as pl
import structlog

from cladetime.exceptions import CladeTimeFutureDateWarning, CladeTimeInvalidDateError, CladeTimeInvalidURLError
from cladetime.util.config import Config
from cladetime.util.reference import _get_s3_object_url
from cladetime.util.sequence import _get_ncov_metadata, get_covid_genome_metadata

logger = structlog.get_logger()


class CladeTime:
    """Interface for Nextstrain SARS-CoV-2 genome sequences and clades.

    The CladeTime class is instantiated with two optional arguments that
    specify the point in time at which to access genome sequences/metadata
    as well as the reference tree used for clade assignment. CladeTime
    interacts with GenBank-based data provided by the Nextstrain project.

    Parameters
    ----------
    sequence_as_of : datetime.datetime | str | None
        Sets the versions of Nextstrain SARS-CoV-2 genome sequence
        and sequence metadata files that will be used by
        CladeTime properties and methods. Can be a datetime object or a
        string in YYYY-MM-DD format, both of which will be treated as
        UTC. The default value is the current time.
    tree_as_of : datetime.datetime | str | None
        Sets the version of the Nextstrain reference tree that will be
        used by CladeTime. Can be a datetime object or a string in
        YYYY-MM-DD format, both of which will be treated as UTC.
        The default value is :any:`sequence_as_of<sequence_as_of>`

    Attributes
    ----------
    url_ncov_metadata : str
        S3 URL to metadata from the Nextstrain pipeline run that
        generated the sequence clade assignments in
        :any:`url_sequence_metadata<url_sequence_metadata>`
    url_sequence : str
        S3 URL to the Nextstrain Sars-CoV-2 sequence file (zst-compressed
        .fasta) that was current at the date specified in
        :any:`sequence_as_of<sequence_as_of>`
    url_sequence_metadata : str
        S3 URL to the Nextstrain Sars-CoV-2 sequence metadata file
        (zst-compressed tsv) that was current at the date specified in
        :any:`sequence_as_of<sequence_as_of>`
    """

    def __init__(self, sequence_as_of=None, tree_as_of=None):
        """CladeTime constructor."""
        self._config = self._get_config()
        self.sequence_as_of = sequence_as_of
        self.tree_as_of = tree_as_of
        self._ncov_metadata = {}
        self._sequence_metadata = pl.LazyFrame()

        self.url_sequence = _get_s3_object_url(
            self._config.nextstrain_ncov_bucket, self._config.nextstrain_genome_sequence_key, self.sequence_as_of
        )[1]
        self.url_sequence_metadata = _get_s3_object_url(
            self._config.nextstrain_ncov_bucket, self._config.nextstrain_genome_metadata_key, self.sequence_as_of
        )[1]

        # Nextstrain began publishing ncov pipeline metadata starting on 2024-08-01
        if self.sequence_as_of >= self._config.nextstrain_min_ncov_metadata_date:
            self.url_ncov_metadata = _get_s3_object_url(
                self._config.nextstrain_ncov_bucket, self._config.nextstrain_ncov_metadata_key, self.sequence_as_of
            )[1]
        else:
            self.url_ncov_metadata = None

    @property
    def sequence_as_of(self) -> datetime:
        """
        datetime.datetime : The date and time (UTC) used to retrieve NextStrain sequences
        and sequence metadata. :any:`url_sequence<url_sequence>` and
        :any:`url_sequence_metadata<url_sequence_metadata>` link to
        Nextstrain files that were current as of this date.
        """
        return self._sequence_as_of

    @sequence_as_of.setter
    def sequence_as_of(self, date) -> None:
        sequence_as_of = self._validate_as_of_date(date)
        utc_now = datetime.now(timezone.utc)
        if sequence_as_of > utc_now:
            warnings.warn(
                f"specified sequence_as_of is in the future, defaulting to current time: {utc_now}",
                category=CladeTimeFutureDateWarning,
            )
            sequence_as_of = utc_now

        self._sequence_as_of = sequence_as_of

    @property
    def tree_as_of(self) -> datetime:
        """
        datetime.datetime : The date and time (UTC) used to retrieve the NextStrain
        reference tree. :any:`get_reference_tree<get_reference_tree>`
        uses this date to get the reference tree that was current as
        of this date.
        """
        return self._tree_as_of

    @tree_as_of.setter
    def tree_as_of(self, date) -> None:
        if date is None:
            tree_as_of = self.sequence_as_of
        else:
            tree_as_of = self._validate_as_of_date(date)
        utc_now = datetime.now(timezone.utc)
        if tree_as_of > utc_now:
            warnings.warn(
                f"specified tree_as_of is in the future, defaulting to sequence_as_of: {self.sequence_as_of}",
                category=CladeTimeFutureDateWarning,
            )
            tree_as_of = self.sequence_as_of

        self._tree_as_of = tree_as_of

    @property
    def ncov_metadata(self):
        return self._ncov_metadata

    @ncov_metadata.getter
    def ncov_metadata(self) -> dict:
        """
        dict : Metadata for the reference tree that was used for SARS-CoV-2
        clade assignments as of :any:`tree_as_of<tree_as_of>`.
        This property will be empty for dates before 2024-08-01, when
        Nextstrain began publishing ncov pipeline metadata.
        """
        if self.url_ncov_metadata:
            metadata = _get_ncov_metadata(self.url_ncov_metadata)
            return metadata
        else:
            metadata = {}
        return metadata

    @property
    def sequence_metadata(self):
        return self._sequence_metadata

    @sequence_metadata.getter
    def sequence_metadata(self) -> pl.LazyFrame:
        """
        :class:`polars.LazyFrame` : A Polars LazyFrame that references
        :any:`url_sequence_metadata<url_sequence_metadata>`
        """
        if self.url_sequence_metadata:
            sequence_metadata = get_covid_genome_metadata(metadata_url=self.url_sequence_metadata)
            return sequence_metadata
        else:
            raise CladeTimeInvalidURLError("CladeTime is missing url_sequence_metadata")

    def __repr__(self):
        return f"CladeTime(sequence_as_of={self.sequence_as_of}, tree_as_of={self.tree_as_of})"

    def __str__(self):
        return f"Work with Nextstrain Sara-CoV-2 sequences as of {self.sequence_as_of} and Nextclade clade assignments as of {self.tree_as_of}"

    def _get_config(self) -> Config:
        """Return a config object."""
        # dates passed to Config don't actually do anything in this case
        # (config needs a refactor)
        config = Config(datetime.now(), datetime.now())

        return config

    def _validate_as_of_date(self, as_of: str) -> datetime:
        """Validate an as_of date used to instantiate CladeTime.

        All dates used to instantiate CladeTime are assigned
        a datetime tzinfo of UTC.
        """
        if as_of is None:
            as_of_date = datetime.now(timezone.utc)
        elif isinstance(as_of, datetime):
            as_of_date = as_of.replace(tzinfo=timezone.utc)
        elif isinstance(as_of, str):
            try:
                as_of_date = datetime.strptime(as_of, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError as e:
                raise CladeTimeInvalidDateError(f"Invalid date string: {as_of} (should be in YYYY-MM-DD format)") from e

        as_of_date = as_of_date.replace(microsecond=0)
        if as_of_date < self._config.nextstrain_min_seq_date:
            raise CladeTimeInvalidDateError(f"Date must be after May 1, 2023: {as_of_date}")

        return as_of_date

    def get_reference_tree(self) -> dict:
        """Return a reference tree used for SARS-CoV-2 clade assignments

        Retrieves the reference tree that was current as of
        :any:`tree_as_of<tree_as_of>`.

        This method is not yet implemented.

        Returns
        -------
        dict
        """
        return {self.tree_as_of: "not implemented"}
