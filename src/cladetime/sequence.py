"""Functions for retrieving and parsing SARS-CoV-2 virus genome data."""

import lzma
import os
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import structlog
import us
from requests import Session

from cladetime.types import StateFormat
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

    metadata = response.json()
    if metadata.get("nextclade_dataset_name", "").lower() == "sars-cov-2":
        metadata["nextclade_dataset_name_full"] = "nextstrain/sars-cov-2/wuhan-hu-1/orfs"

    return metadata


def filter_sequence_metadata(
    metadata: pl.LazyFrame, cols: list | None = None, state_format: StateFormat = StateFormat.ABBR
) -> pl.LazyFrame:
    """Apply standard filters to Nextstrain's SARS-CoV-2 sequence metadata.

    A helper function to apply commonly-used filters to a Polars LazyFrame
    that represents Nextstrain's SARS-CoV-2 sequence metadata. It filters
    on human sequences from the United States (including Puerto Rico and
    Washington, DC).

    This function also performs small transformations to the metadata,
    such as casting the collection date to a date type, renaming columns,
    and returning alternate state formats if requested.

    Parameters
    ----------
    metadata : :class:`polars.LazyFrame`
        The :attr:`cladetime.CladeTime.url_sequence_metadata`
        attribute of a :class:`cladetime.CladeTime` object. This parameter
        represents SARS-CoV-2 sequence metadata produced by Nextstrain
        as an intermediate file in their daily workflow
    cols : list
        Optional. A list of columns to include in the filtered metadata.
        The default columns included in the filtered metadata are:
        clade_nextstrain, country, date, division, genbank_accession,
        genbank_accession_rev, host
    state_format : :class:`cladetime.types.StateFormat`
        Optional. The state name format returned in the filtered metadata's
        location column. Defaults to `StateFormat.ABBR`

    Returns
    -------
    :class:`polars.LazyFrame`
        A Polars LazyFrame that represents the filtered SARS-CoV-2 sequence
        metadata.

    Raises
    ------
    ValueError
        If the state_format parameter is not a valid
        :class:`cladetime.types.StateFormat`.

    Notes
    -----
    This function will filter out metadata rows with invalid state names or
    date strings that cannot be cast to a Polars date format.

    Example:
    --------
    >>> from cladetime import CladeTime
    >>> from cladetime.sequence import filter_covid_genome_metadata

    Apply common filters to the sequence metadata of a CladeTime object:

    >>> ct = CladeTime(seq_as_of="2024-10-15")
    >>> ct = CladeTime(sequence_as_of="2024-10-15")
    >>> filtered_metadata = filter_covid_genome_metadata(ct.sequence_metadata)
    >>> filtered_metadata.collect().head(5)
    shape: (5, 7)
    ┌───────┬─────────┬────────────┬────────────┬────────────┬──────────────┬──────┬
    │ clade ┆ country ┆ date       ┆ genbank_   ┆ genbank_ac ┆ host         ┆ loca │
    │       ┆         ┆            ┆ accession  ┆ cession_rev┆              ┆ tion │
    │ ---   ┆ ---     ┆ ---        ┆ ---        ┆ ---        ┆ ---          ┆ ---  │
    │ str   ┆ str     ┆ date       ┆ str        ┆ str        ┆ str          ┆ str  │
    │       ┆         ┆            ┆            ┆            ┆              ┆      │
    ╞═══════╪═════════╪════════════╪════════════╪════════════╪══════════════╪══════╡
    │ 22A   ┆ USA     ┆ 2022-07-07 ┆ PP223234   ┆ PP223234.1 ┆ Homo sapiens ┆ AL   │
    │ 22B   ┆ USA     ┆ 2022-07-02 ┆ PP223435   ┆ PP223435.1 ┆ Homo sapiens ┆ AZ   │
    │ 22B   ┆ USA     ┆ 2022-07-19 ┆ PP223235   ┆ PP223235.1 ┆ Homo sapiens ┆ AZ   │
    │ 22B   ┆ USA     ┆ 2022-07-15 ┆ PP223236   ┆ PP223236.1 ┆ Homo sapiens ┆ AZ   │
    │ 22B   ┆ USA     ┆ 2022-07-20 ┆ PP223237   ┆ PP223237.1 ┆ Homo sapiens ┆ AZ   │
    └───────┴─────────┴────────────┴────────────┴────────────┴─────────────────────┴
    """
    if state_format not in StateFormat:
        raise ValueError(f"Invalid state_format. Must be one of: {list(StateFormat.__members__.items())}")

    # Default columns to include in the filtered metadata
    if not cols:
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
    states.extend(["Washington DC", "District of Columbia", "Puerto Rico"])

    # Filter dataset and do some general tidying
    filtered_metadata = (
        metadata.select(cols)
        .filter(
            pl.col("country") == "USA",
            pl.col("division").is_in(states),
            pl.col("host") == "Homo sapiens",
        )
        .rename({"clade_nextstrain": "clade"})
        .cast({"date": pl.Date}, strict=False)
        # date filtering at the end ensures we filter out null
        # values created by the above .cast operation
        .filter(
            pl.col("date").is_not_null(),
        )
    )

    # Create state mappings based on state_format parameter, including a DC alias, since
    # Nextrain's metadata uses a different name than the us package
    if state_format == StateFormat.FIPS:
        state_dict = {state.name: state.fips for state in us.states.STATES_AND_TERRITORIES}
        state_dict["Washington DC"] = us.states.DC.fips
    elif state_format == StateFormat.ABBR:
        state_dict = {state.name: state.abbr for state in us.states.STATES_AND_TERRITORIES}
        state_dict["Washington DC"] = us.states.DC.abbr
    else:
        state_dict = {state.name: state.name for state in us.states.STATES_AND_TERRITORIES}
        state_dict["Washington DC"] = "Washington DC"

    filtered_metadata = filtered_metadata.with_columns(pl.col("division").replace(state_dict).alias("location")).drop(
        "division"
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
