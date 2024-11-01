"""Functions for retrieving and parsing SARS-CoV-2 phylogenic tree data."""

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Tuple

import boto3
import docker
import structlog
from botocore import UNSIGNED
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from docker.errors import DockerException

from cladetime.exceptions import NextcladeNotAvailableError

logger = structlog.get_logger()


def _docker_installed():
    """Check if Docker is installed and running."""
    try:
        subprocess.run(["docker", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        docker_enabled = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        msg = (
            "WARNING: Docker is not installed on this machine, or it is not currently running.\n"
            "Cladetime features that require Docker will not be available:\n"
            " - retrieving reference trees\n"
            " - performing custom clade assignments\n"
        )
        print(msg)
        docker_enabled = False

    return docker_enabled


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


def _run_nextclade_cli(nextclade_cli_version: str, nextclade_command: list[str], output_file: Path) -> Path:
    """Invoke Nextclade CLI commands via Docker."""

    try:
        client = docker.from_env()
    except DockerException as err:
        logger.error("Error creating py-docker client", error=err)
        raise NextcladeNotAvailableError(
            "Unable to create client for Nextstrain CLI. Is Docker installed and running?"
        ) from err

    output_path = output_file.parent

    try:
        client.containers.run(
            image=f"nextstrain/nextclade:{nextclade_cli_version}",
            command=nextclade_command,
            volumes={str(output_path): {"bind": "/data/", "mode": "rw"}},
            remove=True,
            tty=True,
        )
    except DockerException as err:
        msg = "Error running Nextclade CLI via Docker"
        logger.error(
            msg,
            cli_version=nextclade_cli_version,
            command=nextclade_command,
            error=err,
        )
        raise NextcladeNotAvailableError(msg) from err

    return output_file


def get_nextclade_dataset(
    nextclade_cli_version: str, dataset_name: str, dataset_version: str, output_path: Path
) -> Path:
    """Return a specific version of a Nextclade dataset.

    Run the Nextclade CLI :external:doc:`dataset get<user/nextclade-cli/usage>`
    command and save the output as a zip file.

    Parameters
    ----------
    nextclade_cli_version : str
        Version of the Nextclade CLI to use
    dataset_name : str
        Name of the Nextclade dataset to retrieve (e.g., "sars-cov-2")
    dataset_version : str
        Nextclade dataset version to retrieve (e.g., "2024-09-25--21-50-30Z")
    output_path : pathlib.Path
        Where to save the Nextclade dataset zip file

    Returns
    -------
    pathlib.Path
        Full path to the Nextclade dataset zip file

    Raises
    ------
    NextcladeNotAvailableError
        If there is an error creating a Docker client or running Nextclade
        CLI commands using the Docker image.
    """
    zip_filename = f"nextclade_{dataset_name}_{dataset_version}.zip"
    output_file = output_path / zip_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        "nextclade",
        "dataset",
        "get",
        "--name",
        dataset_name,
        "--tag",
        dataset_version,
        "--output-zip",
        f"/data/{zip_filename}",
    ]

    _run_nextclade_cli(nextclade_cli_version, command, output_file)

    return output_file
