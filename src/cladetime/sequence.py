"""Functions for retrieving and parsing SARS-CoV-2 virus genome data."""

import io
import lzma
import os
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import requests
import structlog
import us
import zstandard as zstd
from Bio import SeqIO
from Bio.SeqIO import FastaIO
from requests import Session

from cladetime.types import StateFormat
from cladetime.util.reference import _get_date
from cladetime.util.session import _get_session
from cladetime.util.timing import time_function

logger = structlog.get_logger()


@time_function
def _download_from_url(
    session: Session,
    url: str,
    data_path: Path,
) -> Path:
    """Download a file from the specified URL and save it to data_path.

    Parameters
    ----------
    session : Session
        Requests session for making HTTP requests
    url : str
        URL of the file to download
    data_path : Path
        Path where the downloaded file will be saved

    Returns
    -------
    Path
        Path to the downloaded file
    """

    parsed_url = urlparse(url)
    url_filename = os.path.basename(parsed_url.path)
    filename = data_path / url_filename

    data_path.mkdir(parents=True, exist_ok=True)

    with session.get(url, stream=True) as result:
        result.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in result.iter_content(chunk_size=None):
                f.write(chunk)

    return filename


def get_metadata(
    metadata_path: Path | None = None, metadata_url: str | None = None, num_rows: int | None = None
) -> pl.LazyFrame:
    """
    Read GenBank SARS-CoV-2 genome metadata into a Polars LazyFrame.

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
        # get sequence metadata from a URL
        file_suffix = Path(urlparse(metadata_url).path).suffix
        if file_suffix in [".tsv", ".zst"]:
            metadata = pl.scan_csv(metadata_url, separator="\t", n_rows=num_rows, infer_schema_length=100000)
        elif file_suffix == ".xz":
            # pytyon's lzma module doesn't support opening via HTTP, so use requests
            # to download the file in chunks and then decompress it
            with requests.get(metadata_url, stream=True) as response:
                response.raise_for_status()
                decompressor = lzma.LZMADecompressor()
                buffer = BytesIO()
                for chunk in response.iter_content(chunk_size=24576):
                    if chunk:
                        decompressed_chunk = decompressor.decompress(chunk)
                        buffer.write(decompressed_chunk)
                buffer.seek(0)
                metadata = pl.scan_csv(buffer, separator="\t", n_rows=num_rows, infer_schema_length=100000)
        else:
            raise ValueError(f"Unsupported compression type: {file_suffix}")

        return metadata

    if metadata_path:
        # get sequence metadata from a file on local disk
        if (compression_type := metadata_path.suffix) in [".tsv", ".zst"]:
            metadata = pl.scan_csv(metadata_path, separator="\t", n_rows=num_rows)
        elif compression_type == ".xz":
            metadata = pl.read_csv(
                lzma.open(metadata_path), separator="\t", n_rows=num_rows, infer_schema_length=100000
            ).lazy()
        else:
            raise ValueError(f"Unsupported compression type: {compression_type}")

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
    nextclade_version = metadata.get("nextclade_version")
    if nextclade_version:
        match = re.search(r"\b\d+\.\d+\.\d+\b", nextclade_version)
        metadata["nextclade_version_num"] = match.group(0) if match else None

    return metadata


def filter_metadata(
    metadata: pl.DataFrame | pl.LazyFrame,
    cols: list | None = None,
    state_format: StateFormat = StateFormat.ABBR,
    collection_min_date: datetime | None = None,
    collection_max_date: datetime | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """Apply standard filters to Nextstrain's SARS-CoV-2 sequence metadata.

    A helper function to apply commonly-used filters to a Polars DataFrame
    or LazyFrame that represents Nextstrain's SARS-CoV-2 sequence metadata.
    It filters on human sequences from the United States (including Puerto Rico
    and Washington, DC).

    This function also performs small transformations to the metadata,
    such as casting the collection date to a date type, renaming columns,
    and returning alternate state formats if requested.

    Parameters
    ----------
    metadata : :class:`polars.DataFrame` or :class:`polars.LazyFrame`
        A Polars DataFrame or LazyFrame that represents SARS-CoV-2
        sequence metadata produced by Nextstrain as an intermediate file in
        their daily workflow. This parameter is often the
        :attr:`cladetime.CladeTime.url_sequence_metadata` attribute
        of a :class:`cladetime.CladeTime` object
    cols : list
        Optional. A list of columns to include in the filtered metadata.
        The default columns included in the filtered metadata are:
        clade_nextstrain, country, date, division, strain, host
    state_format : :class:`cladetime.types.StateFormat`
        Optional. The state name format returned in the filtered metadata's
        location column. Defaults to `StateFormat.ABBR`
    collection_min_date : datetime.datetime | None
        Optional. Return sequences collected on or after this date.
        Defaults to None (no minimum date filter).
    collection_max_date : datetime.datetime | None
        Optional. Return sequences collected on or before this date.
        Defaults to None (no maximum date filter).

    Returns
    -------
    :class:`polars.DataFrame` or :class:`polars.LazyFrame`
        A Polars object that represents the filtered SARS-CoV-2 sequence
        metadata. The type of returned object will match the type of the
        function's metadata parameter.

    Raises
    ------
    ValueError
        If the state_format parameter is not a valid
        :class:`cladetime.types.StateFormat`.

    Notes
    -----
    This function will filter out metadata rows with invalid state names or
    date strings that cannot be cast to a Polars date format.

    Example
    --------
    >>> from cladetime import CladeTime
    >>> from cladetime.sequence import filter_covid_genome_metadata
    >>>
    >>> ct = CladeTime(seq_as_of="2024-10-15")
    >>> ct = CladeTime(sequence_as_of="2024-10-15")
    >>> filtered_metadata = filter_covid_genome_metadata(ct.sequence_metadata)
    >>> filtered_metadata.collect().head(5)
    shape: (5, 7)
    ┌───────┬─────────┬────────────┬────────────────────────────┬──────────────┬──────┬
    │ clade ┆ country ┆ date       ┆ strain                     ┆ host         ┆ loca │
    │       ┆         ┆            ┆                            ┆              ┆ tion │
    │ ---   ┆ ---     ┆ ---        ┆ ---                        ┆ ---          ┆ ---  │
    │ str   ┆ str     ┆ date       ┆ str                        ┆ str          ┆ str  │
    │       ┆         ┆            ┆                            ┆              ┆      │
    ╞═══════╪═════════╪════════════╪════════════════════════════╪══════════════╪══════╡
    │ 22A   ┆ USA     ┆ 2022-07-07 ┆ Alabama/SEARCH-202312/2022 ┆ Homo sapiens ┆ AL   │
    │ 22B   ┆ USA     ┆ 2022-07-02 ┆ Arizona/SEARCH-201153/2022 ┆ Homo sapiens ┆ AZ   │
    │ 22B   ┆ USA     ┆ 2022-07-19 ┆ Arizona/SEARCH-203528/2022 ┆ Homo sapiens ┆ AZ   │
    │ 22B   ┆ USA     ┆ 2022-07-15 ┆ Arizona/SEARCH-203621/2022 ┆ Homo sapiens ┆ AZ   │
    │ 22B   ┆ USA     ┆ 2022-07-20 ┆ Arizona/SEARCH-203625/2022 ┆ Homo sapiens ┆ AZ   │
    └───────┴─────────┴────────────┴────────────────────────────┴─────────────────────┴
    """
    if state_format not in StateFormat:
        raise ValueError(f"Invalid state_format. Must be one of: {list(StateFormat.__members__.items())}")

    # Default columns to include in the filtered metadata
    if cols is None:
        cols = [
            "clade_nextstrain",
            "country",
            "date",
            "division",
            "strain",
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

    # Apply filters for min and max sequence collection date, if applicable
    if collection_min_date is not None:
        collection_min_date = _get_date(collection_min_date)
        filtered_metadata = filtered_metadata.filter(pl.col("date") >= collection_min_date)
    if collection_max_date is not None:
        collection_max_date = _get_date(collection_max_date)
        filtered_metadata = filtered_metadata.filter(pl.col("date") <= collection_max_date)

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
    """Return a count of clades by location and date.

    Notes:
    ------
    Deprecated in favor of summarize_clades
    """

    cols = [
        "clade",
        "country",
        "date",
        "location",
        "host",
    ]

    counts = filtered_metadata.select(cols).group_by("location", "date", "clade").agg(pl.len().alias("count"))

    return counts


def summarize_clades(sequence_metadata: pl.LazyFrame, group_by: list | None = None) -> pl.LazyFrame:
    """Return clade counts summarized by specific sequence metadata columns.

    Parameters
    ----------
    sequence_metadata : :class:`polars.DataFrame` or :class:`polars.LazyFrame`
        A Polars DataFrame or LazyFrame that represents
        Nextstrain SARS-CoV-2 sequence metadata
    group_by : list
        Optional. A list of columns to group the clade counts by. Defaults
        to ["clade_nextstrain", "country", "date", "location", "host"]

    Returns
    -------
    :class:`polars.DataFrame` | :class:`polars.LazyFrame`
        A Frame that summarizes clade counts by the specified columns. If sequence_metadata
        is a LazyFrame, returns a LazyFrame. Otherwise, returns a DataFrame.

    Notes
    -----
    This function does not validate the group_by columns because doing so on a
    large LazyFrame would involve a memory-intensive collect_schema operation.
    If the group_by columns are not in the sequence metadata, this function
    will succeed, but a subsequent collect() on the returned LazyFrame will
    result in an error.
    """
    if group_by is None:
        group_by = ["clade_nextstrain", "country", "date", "location", "host"]

    counts = (
        sequence_metadata.select(group_by).group_by(group_by).agg(pl.len().alias("count")).cast({"count": pl.UInt32})
    )

    return counts


def get_metadata_ids(sequence_metadata: pl.DataFrame | pl.LazyFrame) -> set:
    """Return sequence IDs for a specified set of Nextstrain sequence metadata.

    For a given input of GenBank-based SARS-Cov-2 sequence metadata (as
    published by Nextstrain), return a set of strains. This function is
    mostly used to filter a sequence file.

    Parameters
    ----------
    sequence_metadata : :class:`polars.DataFrame` or :class:`polars.LazyFrame`

    Returns
    -------
    set
        A set of
        :external+ncov:doc:`strains<reference/metadata-fields>`

    Raises
    ------
    ValueError
        If the sequence metadata does not contain a strain column
    """
    try:
        sequences = sequence_metadata.select("strain").unique()
        if isinstance(sequence_metadata, pl.LazyFrame):
            sequences = sequences.collect()
        seq_set = set(sequences["strain"].to_list())
    except pl.exceptions.ColumnNotFoundError:
        seq_set = set()

    return seq_set


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


@time_function
def filter(sequence_ids: set, url_sequence: str, output_path: Path) -> Path:
    """Filter a fasta file against a specific set of sequences.

    Download a sequence file (in FASTA format) from Nexstrain, filter
    it against a set of specific strains, and write the filtered
    sequences to a new file.

    Parameters
    ----------
    sequence_ids : set
        Strains used to filter the sequence file
    url_sequence : str
        The URL to a file of SARS-CoV-2 GenBank sequences published by Nexstrain.
        The file is should be in .fasta format using the lzma compression
        method (e.g., "https://data.nextstrain.org/files/ncov/open/100k/sequences.fasta.xz")
    output_path : pathlib.Path
        Where to save the filtered sequence file

    Returns
    -------
    pathlib.Path
        Full path to the filtered sequence file

    Raises
    ------
    ValueError
        If url_sequence points to a file that doesn't have a
        .zst or .xz extension.
    """
    session = _get_session()

    # If URL doesn't indicate a file compression format used
    # by Nextstrain, exit before downloading
    parsed_sequence_url = urlparse(url_sequence)
    file_extension = Path(parsed_sequence_url.path).suffix.lower()
    if file_extension not in [".xz", ".zst"]:
        raise ValueError(f"Unsupported compression format: {file_extension}")
    filtered_sequence_file = output_path / "sequences_filtered.fasta"

    logger.info("Downloading sequence file", url=url_sequence)
    sequence_file = _download_from_url(session, url_sequence, output_path)
    logger.info("Sequence file saved", path=sequence_file)

    # create a second fasta file with only those sequences in the metadata
    logger.info("Starting sequence filter", filtered_sequence_file=filtered_sequence_file)
    sequence_count = 0
    sequence_match_count = 0

    with open(filtered_sequence_file, "w") as fasta_output:
        if file_extension == ".xz":
            with lzma.open(sequence_file, mode="rt") as handle:
                for record in FastaIO.FastaIterator(handle):
                    sequence_count += 1
                    if record.id in sequence_ids:
                        sequence_match_count += 1
                        SeqIO.write(record, fasta_output, "fasta")
        else:
            with open(sequence_file, "rb") as handle:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(handle) as reader:
                    text_stream = io.TextIOWrapper(reader, encoding="utf-8")
                    for record in FastaIO.FastaIterator(text_stream):
                        sequence_count += 1
                        if record.id in sequence_ids:
                            sequence_match_count += 1
                            SeqIO.write(record, fasta_output, "fasta")

    logger.info(
        "Filtered sequence file saved",
        num_sequences=sequence_count,
        num_matched_sequences=sequence_match_count,
        path=filtered_sequence_file,
    )

    return filtered_sequence_file
