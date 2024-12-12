"""Functions for retrieving and parsing SARS-CoV-2 phylogenic tree data."""

import subprocess
from datetime import datetime, timezone
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


def _get_date(original_date: datetime | str | None) -> datetime:
    """Validate an as_of date used to instantiate CladeTime.

    All CladeTime dates are assigned a datetime tzinfo of UTC.
    """
    if original_date is None:
        new_date = datetime.now(timezone.utc)
    elif isinstance(original_date, datetime):
        new_date = original_date.replace(tzinfo=timezone.utc)
    elif isinstance(original_date, str):
        try:
            new_date = datetime.strptime(original_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {original_date}") from e

    new_date = new_date.replace(microsecond=0)

    return new_date


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


def _run_nextclade_cli(
    nextclade_cli_version: str, nextclade_command: list[str], output_path: Path, input_files: list[Path] | None = None
) -> Path:
    """Invoke Nextclade CLI commands via Docker."""

    try:
        client = docker.from_env()
    except DockerException as err:
        logger.error("Error creating py-docker client", error=err)
        raise NextcladeNotAvailableError(
            "Unable to create client for Nextstrain CLI. Is Docker installed and running?"
        ) from err

    volumes = {str(output_path): {"bind": "/data/", "mode": "rw"}}

    # if the nextclade command requires input files, add those to the volumes
    # dictionary so they can be mounted in the Docker container
    if input_files:
        for file in input_files:
            volumes[str(file)] = {"bind": f"/data/{file.name}", "mode": "rw"}

    image = f"nextstrain/nextclade:{nextclade_cli_version}"
    try:
        client.containers.run(
            image=image,
            command=nextclade_command,
            volumes=volumes,
            remove=True,
            tty=True,
        )
    except DockerException as err:
        msg = "Error running Nextclade CLI via Docker"
        logger.error(
            msg,
            image=image,
            command=nextclade_command,
            volumes=volumes,
            error=err,
        )
        raise NextcladeNotAvailableError(msg) from err


def _get_nextclade_dataset(
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

    _run_nextclade_cli(nextclade_cli_version, command, output_path)

    return output_file


def _get_clade_assignments(
    nextclade_cli_version: str, sequence_file: Path, nextclade_dataset: Path, output_file: Path
) -> Path:
    """Assign clades to sequences using the Nextclade CLI.

    Invoke the Nextclade CLI :external:doc:`dataset run<user/nextclade-cli/usage>`
    command and save the resulting clade assignment file to disk. The clade
    assignment file will be in TSV format.

    Parameters
    ----------
    nextclade_cli_version : str
        Version of the Nextclade CLI to use. Used as the tag when
        pulling the Nextclade CLI Docker image (e.g., "3.8.2")
    sequence_file : pathlib.Path
        Location of the sequence file to assign clades to. The file should
        be in FASTA format.
    nextclade_dataset : pathlib.Path
        Location of the :external:doc:`Nextclade dataset<user/datasets>`
        that contains the reference tree and root sequence to use
        for clade assignment. Use :func:`get_nextclade_dataset` to
        get a dataset that corresponds to a specific point in time.
    output_file : pathlib.Path
        The full filename to use for saving the clade assignment output.

    Returns
    -------
    pathlib.Path
        Full path to the
        :external:doc:`clade assignment file`<user/output-files/04-results-tsv>
        created by Nextclade

    Raises
    ------
    NextcladeNotAvailableError
        If there is an error creating a Docker client or running Nextclade
        CLI commands using the Docker image.
    """
    if not output_file.suffix:
        raise ValueError("output_file should be a full path to the output file, including filename")
    output_path = output_file.parent
    output_path.mkdir(parents=True, exist_ok=True)
    assignment_filename = output_file.name

    # all files in the input_files list will be mounted to
    # the docker image's "/data/" directory when running
    # commands (the Nextclade CLI needs the sequence file
    # and nextclade_dataset file to do clade assignment)
    input_files = [sequence_file, nextclade_dataset]

    command = [
        "nextclade",
        "run",
        "--input-dataset",
        f"/data/{nextclade_dataset.name}",
        "--output-tsv",
        f"/data/{assignment_filename}",
        f"/data/{sequence_file.name}",
    ]

    _run_nextclade_cli(nextclade_cli_version, command, output_path, input_files=input_files)

    return output_file
