"""Functions for retrieving and parsing SARS-CoV-2 virus genome data."""

import lzma
import os
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import structlog
import us
from requests import Session

from cladetime.util.session import _get_session
from cladetime.util.timing import time_function

logger = structlog.get_logger()


@time_function
def _download_from_url(session: Session, url: str, data_path: Path) -> Path:
    """Download a file from the specified URL and save it to data_path."""

    parsed_url = urlparse(url)
    url_filename = os.path.basename(parsed_url.path)
    filename = data_path / url_filename

    with session.get(url, stream=True) as result:
        result.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in result.iter_content(chunk_size=None):
                f.write(chunk)

    return filename


def get_covid_genome_metadata(
    metadata_path: Path | None = None, metadata_url: str | None = None, num_rows: int | None = None
) -> pl.LazyFrame:
    """
    Read GenBank genome metadata into a Polars LazyFrame.

    Parameters
    ----------
    metadata_path : Path | None
        Path to location of a NextStrain GenBank genome metadata file.
        Cannot be used with metadata_url.
    metadata_url: str | None
        URL to a NextStrain GenBank genome metadata file.
        Cannot be used with metadata_path.
    num_rows : int | None, default = None
        The number of genome metadata rows to request.
        When not supplied, request all rows.
    """

    path_flag = metadata_path is not None
    url_flag = metadata_url is not None

    assert path_flag + url_flag == 1, "Specify metadata_path or metadata_url, but not both."

    if metadata_url:
        metadata = pl.scan_csv(metadata_url, separator="\t", n_rows=num_rows)
        return metadata

    if metadata_path:
        if (compression_type := metadata_path.suffix) in [".tsv", ".zst"]:
            metadata = pl.scan_csv(metadata_path, separator="\t", n_rows=num_rows)
        elif compression_type == ".xz":
            metadata = pl.read_csv(
                lzma.open(metadata_path), separator="\t", n_rows=num_rows, infer_schema_length=100000
            ).lazy()

    return metadata


def _get_ncov_metadata(
    url_ncov_metadata: str,
    session: Session | None = None,
) -> dict:
    """Return metadata emitted by the Nextstrain ncov pipeline."""
    if not session:
        session = _get_session(retry=False)

    response = session.get(url_ncov_metadata)
    if not response.ok:
        logger.warn(
            "Failed to retrieve ncov metadata",
            status_code=response.status_code,
            response_text=response.text,
            request=response.request.url,
            request_body=response.request.body,
        )
        return {}

    return response.json()


def filter_covid_genome_metadata(metadata: pl.LazyFrame, cols: list = []) -> pl.LazyFrame:
    """Apply a standard set of filters to the GenBank genome metadata."""

    # Default columns to include in the filtered metadata
    if len(cols) == 0:
        cols = [
            "clade_nextstrain",
            "country",
            "date",
            "division",
            "genbank_accession",
            "genbank_accession_rev",
            "host",
        ]

    # There are some other odd divisions in the data, but these are 50 states, DC and PR
    states = [state.name for state in us.states.STATES]
    states.extend(["Washington DC", "Puerto Rico"])

    # Filter dataset and do some general tidying
    filtered_metadata = (
        metadata.select(cols)
        .filter(
            pl.col("country") == "USA",
            pl.col("division").is_in(states),
            pl.col("date").is_not_null(),
            pl.col("host") == "Homo sapiens",
        )
        .rename({"clade_nextstrain": "clade", "division": "location"})
        .cast({"date": pl.Date}, strict=False)
    )

    return filtered_metadata


def get_clade_counts(filtered_metadata: pl.LazyFrame) -> pl.LazyFrame:
    """Return a count of clades by location and date."""

    cols = [
        "clade",
        "country",
        "date",
        "location",
        "host",
    ]

    counts = filtered_metadata.select(cols).group_by("location", "date", "clade").agg(pl.len().alias("count"))

    return counts


def parse_sequence_assignments(df_assignments: pl.DataFrame) -> pl.DataFrame:
    """Parse out the sequence number from the seqName column returned by the clade assignment tool."""

    # polars apparently can't split out the sequence number from that big name column
    # without resorting an apply, so here we're dropping into pandas to do that
    # (might be a premature optimization, since this manoever requires both pandas and pyarrow)
    seq = pl.from_pandas(df_assignments.to_pandas()["seqName"].str.split(" ").str[0].rename("seq"))

    # we're expecting one row per sequence
    if seq.n_unique() != df_assignments.shape[0]:
        raise ValueError("Clade assignment data contains duplicate sequence. Stopping assignment process.")

    # add the parsed sequence number as a new column
    df_assignments = df_assignments.insert_column(1, seq)  # type: ignore

    return df_assignments
