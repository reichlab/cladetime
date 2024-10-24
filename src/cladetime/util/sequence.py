"""Functions for retrieving and parsing SARS-CoV-2 virus genome data."""

import json
import lzma
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import structlog
import us
from requests import Session

from cladetime.util.reference import _get_s3_object_url
from cladetime.util.session import _check_response, _get_session
from cladetime.util.timing import time_function

logger = structlog.get_logger()


@time_function
def get_covid_genome_data(released_since_date: str, base_url: str, filename: str):
    """
    Download genome data package from NCBI.
    FIXME: Download the Nextclade-processed GenBank sequence data (which originates from NCBI)
    from https://data.nextstrain.org/files/ncov/open/sequences.fasta.zst instead of using
    the NCBI API.
    """
    headers = {
        "Accept": "application/zip",
    }
    session = _get_session()
    session.headers.update(headers)

    # TODO: this might be a better as an item in the forthcoming config file
    request_body = {
        "released_since": released_since_date,
        "taxon": "SARS-CoV-2",
        "refseq_only": False,
        "annotated_only": False,
        "host": "Homo sapiens",
        "complete_only": False,
        "table_fields": ["unspecified"],
        "include_sequence": ["GENOME"],
        "aux_report": ["DATASET_REPORT"],
        "format": "tsv",
        "use_psg": False,
    }

    logger.info("NCBI API call starting", released_since_date=released_since_date)

    response = session.post(base_url, data=json.dumps(request_body), timeout=(300, 300))
    _check_response(response)

    # Originally tried saving the NCBI package via a stream call and iter_content (to prevent potential
    # memory issues that can arise when download large files). However, ran into an intermittent error:
    # ChunkedEncodingError(ProtocolError('Response ended prematurely').
    # We may need to revisit this at some point, depending on how much data we place to request via the
    # API and what kind of machine the pipeline will run on.
    with open(filename, "wb") as f:
        f.write(response.content)


@time_function
def download_covid_genome_metadata(
    session: Session, bucket: str, key: str, data_path: Path, as_of: str | None = None, use_existing: bool = False
) -> Path:
    """Download the latest GenBank genome metadata data from Nextstrain."""

    if as_of is None:
        as_of_datetime = datetime.now().replace(tzinfo=timezone.utc)
    else:
        as_of_datetime = datetime.strptime(as_of, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    (s3_version, s3_url) = _get_s3_object_url(bucket, key, as_of_datetime)
    filename = data_path / f"{as_of_datetime.date().strftime('%Y-%m-%d')}-{Path(key).name}"

    if use_existing and filename.exists():
        logger.info("using existing genome metadata file", metadata_file=str(filename))
        return filename

    logger.info("starting genome metadata download", source=s3_url, destination=str(filename))
    with session.get(s3_url, stream=True) as result:
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
            pl.col("host") == "Homo sapiens",
        )
        .rename({"clade_nextstrain": "clade", "division": "location"})
        .cast({"date": pl.Date}, strict=False)
        # date filtering at the end ensures we filter out null
        # values created by the above .cast operation
        .filter(
            pl.col("date").is_not_null(),
        )
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


def _unzip_sequence_package(filename: Path, data_path: Path):
    """Unzip the downloaded virus genome data package."""
    with zipfile.ZipFile(filename, "r") as package_zip:
        zip_contents = package_zip.namelist()
        is_metadata = next((s for s in zip_contents if "data_report" in s), None)
        is_sequence = next((s for s in zip_contents if "genomic" in s), None)
        if is_metadata and is_sequence:
            package_zip.extractall(data_path)
        else:
            logger.error("NCBI package is missing expected files", zip_contents=zip_contents)
            # Exit the pipeline without displaying a traceback
            raise SystemExit("Error downloading NCBI package")


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
