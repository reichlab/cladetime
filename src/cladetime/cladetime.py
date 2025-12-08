"""Class for clade time traveling."""

import tempfile
import warnings
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import structlog

from cladetime import Tree, sequence
from cladetime.clade import Clade
from cladetime.exceptions import (
    CladeTimeDataUnavailableError,
    CladeTimeDateWarning,
    CladeTimeInvalidURLError,
    CladeTimeSequenceWarning,
)
from cladetime.util.config import Config
from cladetime.util.reference import _get_clade_assignments, _get_date, _get_nextclade_dataset, _get_s3_object_url

logger = structlog.get_logger()


class CladeTime:
    """Interface for Nextstrain SARS-CoV-2 genome sequences and clades.

    The CladeTime class is instantiated with two optional arguments that
    specify the point in time at which to access genome sequences/metadata
    as well as the reference tree used for clade assignment. CladeTime
    interacts with GenBank-based data provided by the Nextstrain project.

    Important
    ---------
    Historical data availability is constrained by Nextstrain's infrastructure:

    - sequence_as_of: Must be >= 2025-09-29 (Nextstrain S3 90 day retention)
    - tree_as_of: Must be >= 2024-10-09 (variant-nowcast-hub archive availability)

    These constraints reflect Nextstrain's October 2025 implementation of a
    90 day retention policy for S3 versioned objects. Dates outside these
    windows will raise CladeTimeDataUnavailableError. See GitHub issue #185
    for details and potential workarounds.

    Note: These limitations may change as Nextstrain's infrastructure evolves.

    Parameters
    ----------
    sequence_as_of : datetime.datetime | str | None
        Sets the versions of Nextstrain SARS-CoV-2 genome sequence and
        sequence metadata files that will be used by CladeTime
        properties and methods. Can be a datetime object or a
        string in YYYY-MM-DD format, both of which will be treated as
        UTC. The default value is the current UTC time. Dates passed
        as YYYY-MM-DD strings will be set to 11:59:59 PM UTC.
        Must be >= 2025-09-29.
    tree_as_of : datetime.datetime | str | None
        Sets the version of the Nextstrain reference tree that will be
        used by CladeTime. Can be a datetime object or a string in
        YYYY-MM-DD format, both of which will be treated as UTC.
        The default value is :any:`sequence_as_of<sequence_as_of>`.
        Dates passed as YYYY-MM-DD strings will be set to 11:59:59 PM UTC.
        Must be >= 2024-10-09.

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
            try:
                self.url_ncov_metadata = _get_s3_object_url(
                    self._config.nextstrain_ncov_bucket, self._config.nextstrain_ncov_metadata_key, self.sequence_as_of
                )[1]
            except ValueError as e:
                # S3 doesn't have historical metadata - will use Hub fallback when fetching
                logger.warn(
                    "Nextstrain S3 metadata not available, will use Hub fallback",
                    date=self.sequence_as_of.strftime("%Y-%m-%d"),
                    error=str(e),
                )
                # Set to empty string so fallback will be triggered
                self.url_ncov_metadata = ""
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
        min_sequence_date = self._config.nextstrain_min_seq_date
        date_warning = False
        utc_now = datetime.now(timezone.utc)

        try:
            sequence_as_of = _get_date(date)
        except ValueError:
            sequence_as_of = utc_now
            date_warning = True

        # Check if date is before data availability window - raise error
        if sequence_as_of < min_sequence_date:
            raise CladeTimeDataUnavailableError(
                f"\nSequence data is not available before {min_sequence_date.strftime('%Y-%m-%d')}. "
                f"Nextstrain S3 only retains up to 90 days of historical versions. "
                f"Requested date: {sequence_as_of.strftime('%Y-%m-%d')}. "
                f"\nNote: This limitation is due to Nextstrain's data retention policy, "
                f"which may change over time. See GitHub issue #185 for more details."
            )
        elif sequence_as_of > utc_now:
            sequence_as_of = utc_now
            date_warning = True

        if date_warning:
            msg = (
                "\nSequence as_of cannot be in the future, defaulting to "
                f"current date: {sequence_as_of.strftime('%Y-%m-%d')}"
            )
            warnings.warn(msg, category=CladeTimeDateWarning)

        self._sequence_as_of = sequence_as_of

    @property
    def tree_as_of(self) -> datetime:
        """
        datetime.datetime : The date and time (UTC) used to retrieve the NextStrain
        reference tree.
        """
        return self._tree_as_of

    @tree_as_of.setter
    def tree_as_of(self, date) -> None:
        min_tree_date = self._config.nextstrain_min_ncov_metadata_date
        date_warning = False

        if date is None:
            tree_as_of = self.sequence_as_of
        else:
            try:
                tree_as_of = _get_date(date)
            except ValueError:
                date_warning = True
                default_field = "sequence_as_of"
                tree_as_of = self.sequence_as_of

        utc_now = datetime.now(timezone.utc)

        # Check if date is before reference tree metadata availability - raise error
        if tree_as_of < min_tree_date:
            raise CladeTimeDataUnavailableError(
                f"\nReference tree metadata is not available before {min_tree_date.strftime('%Y-%m-%d')}. "
                f"Historical metadata is provided by variant-nowcast-hub archives starting from this date. "
                f"Requested date: {tree_as_of.strftime('%Y-%m-%d')}. "
                f"\nNote: This limitation is due to hub archive availability, which may expand over time. "
                f"See GitHub issue #185 for more details."
            )
        elif tree_as_of > utc_now:
            default_field = "current date"
            date_warning = True
            tree_as_of = utc_now

        if date_warning:
            msg = (
                "\nTree as_of cannot be in the future, defaulting to "
                f"{default_field}: {tree_as_of.strftime('%Y-%m-%d')}"
            )
            warnings.warn(msg, category=CladeTimeDateWarning)

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
        if self.url_ncov_metadata is not None:
            # Pass sequence_as_of date for Hub fallback support
            # Note: empty string "" is valid here - it triggers fallback in _get_ncov_metadata
            metadata = sequence._get_ncov_metadata(self.url_ncov_metadata, as_of_date=self.sequence_as_of)
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
        :external+polars:std:doc:`polars.LazyFrame<reference/lazyframe/index>` : A Polars LazyFrame that references
        :any:`url_sequence_metadata<url_sequence_metadata>`
        """
        if self.url_sequence_metadata:
            sequence_metadata = sequence.get_metadata(metadata_url=self.url_sequence_metadata)
            return sequence_metadata
        else:
            raise CladeTimeInvalidURLError("CladeTime is missing url_sequence_metadata")

    def __repr__(self):
        return f"CladeTime(sequence_as_of={self.sequence_as_of}, tree_as_of={self.tree_as_of})"

    def __str__(self):
        return f"Work with Nextstrain Sara-CoV-2 sequences as of {self.sequence_as_of} and Nextclade clade assignments as of {self.tree_as_of}"

    def _get_config(self) -> Config:
        """Return a config object."""
        config = Config()

        return config

    def assign_clades(self, sequence_metadata: pl.LazyFrame, output_file: Path | str | None = None) -> Clade:
        """Assign clades to a specified set of sequences.

        For each sequence in a sequence file (.fasta), assign a Nextstrain
        clade using the Nextclade reference tree that corresponds to the
        tree_as_of date. The earliest available tree_as_of date is 2024-08-01,
        when Nextstrain began publishing the pipeline metadata that Cladetime
        uses to retrieve past reference trees.

        Parameters
        ----------
        sequence_metadata : polars.LazyFrame
            A Polars LazyFrame of the Nexstrain
            :external+ncov:doc:`sequence metadata<reference/metadata-fields>`
            to use for clade assignment.
        output_file : str | None
            The full path (including a .tsv filename) to where the clade
            assignment output file will be saved. The default value is
            <home_dir>/cladetime/clade_assignments.tsv.

        Returns
        -------
        :class:`cladetime.clade.Clade`
            A Clade object that contains detailed and summarized information
            about clades assigned to the sequences in sequence_metadata.

        Raises
        -------
        CladeTimeSequenceWarning
            If sequence_metadata is empty, the clade assignment process
            will be stopped.

        Example
        -------
        >>> import polars as pl
        >>>
        >>> from cladetime import CladeTime, sequence
        >>> ct = CladeTime(sequence_as_of="2024-11-15", tree_as_of="2024-09-01")
        >>>
        >>> filtered_metadata = sequence.filter_metadata(
        >>>     ct.sequence_metadata,
        >>>     collection_min_date = "2024-10-01",
        >>> )
        >>> clade_assignments = ct.assign_clades(filtered_metadata)
        >>>
        >>> clade_assignment_summary = clade_assignments.summary
        >>> clade_assignment_summary.select(
        >>>     ["location", "date", "clade_nextstrain", "count"])
        >>>     .sort("count", descending=True)
        >>>     .collect(stream=True).head()
        ┌──────────┬────────────┬──────────────────┬───────┐
        │ location ┆ date       ┆ clade_nextstrain ┆ count │
        │ ---      ┆ ---        ┆ ---              ┆ ---   │
        │ str      ┆ date       ┆ str              ┆ u32   │
        ╞══════════╪════════════╪══════════════════╪═══════╡
        │ NY       ┆ 2024-10-01 ┆ 24C              ┆ 15    │
        │ NY       ┆ 2024-10-15 ┆ 24C              ┆ 15    │
        │ NY       ┆ 2024-10-03 ┆ 24C              ┆ 14    │
        │ NY       ┆ 2024-10-14 ┆ 24C              ┆ 14    │
        │ NJ       ┆ 2024-10-16 ┆ 24C              ┆ 12    │
        └──────────┴────────────┴──────────────────┴───────┘
        """
        assignment_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        if output_file is not None:
            output_file = Path(output_file)
        else:
            output_file = Path.home() / "cladetime" / "clade_assignments.tsv"

        logger.info(
            "Starting clade assignment pipeline", sequence_as_of=self.sequence_as_of, tree_as_of=self.tree_as_of
        )

        # drop any clade-related columns from sequence_metadata (if any exists, it will be replaced
        # by the results of the clade assignment)
        logger.info("Removing current sequence assignments from metadata")
        sequence_metadata = sequence_metadata.drop(
            [
                col
                for col in sequence_metadata.collect_schema().names()
                if col not in self._config.nextstrain_standard_metadata_fields
            ]
        )

        # from the sequence metadata, derive a set of sequence IDs (the "strain")
        # column for use when filtering sequences in the .fasta file
        logger.info("Collecting sequence IDs from metadata")
        ids: set = sequence.get_metadata_ids(sequence_metadata)
        sequence_count = len(ids)

        # if there are no sequences in the filtered metadata, stop the clade assignment
        if sequence_count == 0:
            msg = "Sequence_metadata is empty or missing 'strain' columns \n" "Stopping clade assignment...."
            warnings.warn(
                msg,
                category=CladeTimeSequenceWarning,
            )
            return Clade(meta={}, detail=pl.LazyFrame(), summary=pl.LazyFrame())
        else:
            logger.info("Sequence count complete", sequence_count=sequence_count)

        # if there are many sequences in the filtered metadata, warn that clade assignment will
        # take a long time and require a lot of resources
        if sequence_count > self._config.clade_assignment_warning_threshold:
            msg = (
                f"About to assign clades to {sequence_count} sequences. \n" 
                "The assignment process is resource intensive. \n"
                "Depending on the limitations of your machine, \n"
                "you may want to use a smaller subset of sequences."
            )
            warnings.warn(
                msg,
                category=CladeTimeSequenceWarning,
            )

        tree = Tree(self.tree_as_of, self.url_sequence)

        with tempfile.TemporaryDirectory() as tmpdir:
            filtered_sequences = sequence.filter(ids, self.url_sequence, Path(tmpdir))
            nextclade_dataset = _get_nextclade_dataset(
                tree.ncov_metadata.get("nextclade_version_num", ""),
                tree.ncov_metadata.get("nextclade_dataset_name", "").lower(),
                tree.ncov_metadata.get("nextclade_dataset_version", ""),
                Path(tmpdir),
            )
            logger.info(
                "Assigning clades",
                sequences_to_assign=len(ids),
                nextclade_dataset_version=tree.ncov_metadata.get("nextclade_dataset_version"),
            )
            assignments = _get_clade_assignments(
                tree.ncov_metadata.get("nextclade_version_num", ""), filtered_sequences, nextclade_dataset, output_file
            )
            assigned_clades_df = pl.read_csv(assignments, separator="\t", infer_schema_length=100000)
            # get a count of non-null clade_nextstrain values
            # (this is the number of sequences that were assigned to a clade)
            assigned_sequence_count = assigned_clades_df.select(pl.count("clade_nextstrain")).to_series().to_list()[0]

            logger.info(
                "Nextclade assignments done",
                sequences_to_assign=sequence_count,
                sequences_assigned=assigned_sequence_count,
                assignment_file=assignments,
                nextclade_dataset=tree.ncov_metadata.get("nextclade_dataset_version"),
            )

        # join the assigned clades with the original sequence metadata, create a summarized LazyFrame
        # of clade counts by location, date, and host, and return both (along with metadata) in a
        # Clade object
        assigned_clades = sequence_metadata.join(
            assigned_clades_df.lazy(), left_on="strain", right_on="seqName", how="left"
        )
        summarized_clades = sequence.summarize_clades(
            assigned_clades, group_by=["location", "date", "host", "clade_nextstrain", "country"]
        )

        metadata = {
            "sequences_to_assign": sequence_count,
            "sequences_assigned": assigned_sequence_count,
            "sequence_as_of": self.sequence_as_of,
            "tree_as_of": self.tree_as_of,
            "nextclade_dataset_version": tree.ncov_metadata.get("nextclade_dataset_version"),
            "nextclade_dataset_name": tree.ncov_metadata.get("nextclade_dataset_name"),
            "nextclade_version_num": tree.ncov_metadata.get("nextclade_version_num"),
            "assignment_as_of": assignment_date,
        }
        metadata_clades = Clade(meta=metadata, detail=assigned_clades, summary=summarized_clades)

        return metadata_clades
