# mypy: ignore-errors

import datetime
import os
import subprocess
import tempfile
from pathlib import Path

import polars as pl
import rich_click as click
import structlog

from cladetime import CladeTime, sequence
from cladetime.util.config import Config
from cladetime.util.session import _get_session
from cladetime.util.timing import time_function

logger = structlog.get_logger()
session = _get_session()


def _setup_config(base_data_dir: str) -> Config:
    """Return an initialized Config class for the pipeline run."""

    config = Config(
        data_path_root=base_data_dir,
    )

    return config


def _save_sequences(ct: CladeTime, tmpdir: Path) -> Path:
    """Download and save SAR-CoV-2 sequences from Nextstrain."""

    logger.info("Downloading SARS-CoV-2 sequences from Nextstrain", url=ct.url_sequence)
    full_sequence_file = sequence._download_from_url(session=session, url=ct.url_sequence, data_path=Path(tmpdir))
    return full_sequence_file


def _save_tree(tree: dict, tmpdir: Path) -> Path:
    """Save a reference tree to disk and return the path."""

    return Path.home()


def get_sequence_metadata(metadata: pl.DataFrame, sequence_collection_date: datetime.date) -> pl.DataFrame:
    """Download SARS-CoV-2 sequence metadata from Nextstrain."""

    # FIXME: the columns we want from the Nextrain metadata are those on on their "standard metata" list
    # https://docs.nextstrain.org/projects/ncov/en/latest/reference/metadata-fields.html
    cols = [
        "clade_nextstrain",
        "country",
        "date",
        "division",
        "strain",
        "host",
    ]

    # clean and filter metadata (same process used to generate the weekly clade list)
    filtered_metadata = sequence.filter_metadata(metadata, cols)

    # add filters based on user input
    filtered_metadata = filtered_metadata.filter(pl.col("date") >= sequence_collection_date)

    return filtered_metadata


def filter_sequences(filtered_metadata: pl.LazyFrame, full_sequence_file: Path, tmpdir: Path) -> Path:
    """Create input sequence file for clade assignment."""

    # This is where we are going to use biopython to filter the sequence file to a smaller version
    # that only includes sequences in the filtered metadata.
    # Current thinking is that the sequence filtering itself will be a CladeTime method called here
    return Path.home()


def assign_clades(tree_path: Path, sequence_path: Path):
    """Assign downloaded genbank sequences to a clade."""

    # FIXME: restore the nextclade run invocation once we've refactored
    # the code for creating its inputs.
    # The code below that actually invokes the nextclade will likely move to a CladeTime method
    subprocess.run(
        [
            "nextclade",
            "--version",
        ]
    )

    logger.info("Assigned sequences to clades via Nextclade CLI", output_file="some path stuff")


def merge_metadata() -> pl.DataFrame:
    """Merge sequence metadata with clade assignments."""

    # FIXME: this will all be different now
    # Seems like another candidate for CladeTime, to be invoked here
    return pl.DataFrame()


@click.command()
@click.option(
    "--sequence-collection-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    prompt="Include SARS CoV-2 genome data released on or after this date (YYYY-MM-DD)",
    required=True,
    help="Limit the downloaded SARS CoV-2 package to sequences released on or after this date (YYYY-MM-DD format)",
)
@click.option(
    "--tree-as-of",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    prompt="The reference tree as of date (YYYY-MM-DD)",
    required=True,
    help="Reference tree date to use for clade assignments (YYYY-MM-DD format)",
)
@click.option(
    "--data-dir",
    prompt="Directory where the clade assignment file will be saved",
    required=False,
    prompt_required=False,
    default=None,
    help="Directory where the clade assignment file will be saved. Default: [home dir]/covid_variant/",
)
@time_function
def main(sequence_collection_date: datetime.date, tree_as_of: datetime.date, data_dir: str | None):
    # TODO: update CLI options as discussed, including an option to save the linefile

    config = _setup_config(data_dir)
    logger.info("Starting pipeline", reference_tree_date=tree_as_of, run_time=config.run_time)

    os.makedirs(config.data_path, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        ### The lines in this context manager mock out a newer, non-NCBI-based
        ### approach to a cladetime CLI that allows custom clade assignments.
        ### Some of the steps are implemented, since the supporting code already
        ### exists in cladetime. Some of the steps are placeholders to be completed
        ### in future pull requests.

        ct = CladeTime()
        filtered_metadata: pl.DataFrame = get_sequence_metadata(ct.sequence_metadata, sequence_collection_date)
        filtered_metadata.sink_parquet(Path(tmpdir) / "filtered_metadata.parquet", maintain_order=False)
        full_sequence_file = _save_sequences(ct, tmpdir)
        logger.info("Temp sequence file saved", sequence_file=full_sequence_file)
        filtered_sequence_file = filter_sequences(filtered_metadata, full_sequence_file, Path(tmpdir))
        # once tree PR is merged: ref_tree = Tree(ct, tree_as_of).tree
        ref_tree = {}
        tree_file = _save_tree(ref_tree, Path(tmpdir))
        assign_clades(tree_path=tree_file, sequence_path=filtered_sequence_file)
        merged_metadata = merge_metadata()
        # counts = get_clade_counts(merged_metadata)
        return merged_metadata

    logger.info(
        "Sequence clade assignments are ready",
        run_time=config.run_time,
        reference_tree_date=tree_as_of,
    )


if __name__ == "__main__":
    main()
