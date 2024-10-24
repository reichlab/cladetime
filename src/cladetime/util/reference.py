"""Functions for retrieving and parsing SARS-CoV-2 phylogenic tree data."""

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Tuple

import boto3
import structlog
from botocore import UNSIGNED
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

logger = structlog.get_logger()


def get_nextclade_dataset(as_of_date: str, data_path_root: str) -> Path:
    """
    Return the Nextclade dataset relevant to a specified as_of_date.

    The dataset is in .zip format and contains two components required for
    assignming virus genome sequences to clades: a tree and the reference
    sequence of the virus.

    Note:
    -----
    Deprecated? Cladetime.tree uses a different method for retrieving a
    tree "as_of". That might now be reliable, however, so keeping this
    here.

    """

    # Until Nextstrain provides this information, we're hard-coding a
    # a specific version of the nextclade dataset here.
    as_of_date = "not yet implemented"
    DATASET_VERSION = "2024-07-17--12-57-03Z"
    DATASET_PATH = Path(f"{data_path_root}/nextclade_dataset_{DATASET_VERSION}.zip")

    subprocess.run(
        [
            "nextclade",
            "dataset",
            "get",
            "--name",
            "sars-cov-2",
            "--tag",
            DATASET_VERSION,
            "--output-zip",
            str(DATASET_PATH),
        ]
    )

    logger.info(
        "Nextclade reference dataset retrieved", as_of_date=as_of_date, version=DATASET_VERSION, output_zip=DATASET_PATH
    )

    return DATASET_PATH


def _get_s3_object_url(bucket_name: str, object_key: str, date: datetime) -> Tuple[str, str]:
    """
    For a versioned, public S3 bucket and object key, return the version ID
    of the object as it existed at a specific date (UTC)
    """
    try:
        s3_client = boto3.client("s3", config=boto3.session.Config(signature_version=UNSIGNED))  # type: ignore

        paginator = s3_client.get_paginator("list_object_versions")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=object_key)

        selected_version = None
        for page in page_iterator:
            for version in page.get("Versions", []):
                version_date = version["LastModified"]
                if version_date <= date:
                    if selected_version is None or version_date > selected_version["LastModified"]:
                        selected_version = version
    except (BotoCoreError, ClientError, NoCredentialsError) as e:
        logger.error("S3 client error", error=e)
        raise e
    except Exception as e:
        logger.error("Unexpected error", error=e)
        raise e

    if selected_version is None:
        raise ValueError(f"No version of {object_key} found before {date}")

    version_id = selected_version["VersionId"]
    version_url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}?versionId={version_id}"

    return version_id, version_url
