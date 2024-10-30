# Cladetime

Cladetime is a wrapper around existing tools for downloading and working with virus genome sequences:

* The [NCBI Datasets API](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/rest-api/) provided by the National Institutes of Healh.
* The [Nexclade command line interface (CLI)](https://docs.nextstrain.org/projects/nextclade/en/stable/user/nextclade-cli/index.html) provided by [Nextstrain](https://docs.nextstrain.org/en/latest/).

This package was developed to provide data required for the [COVID-19 Variant Nowcast hub](https://github.com/reichlab/variant-nowcast-hub), where modelers and teams forecast the daily proportions of COVID-19 variants in US states.

We are releasing `cladetime` as a standalone package for use by others who may find the functionality useful.


## Usage

This library contains two types of components:

1. Scripts and CLI tools for use in database pipelinnes required by the Variant Nowcast Hub
(these are in development and not documented here).

2. A CladeTime class for interactively working with Sars-Cov-2 sequence and clade data as of a specific date.

### Sample CladeTime usage

To use the interactive `CladeTime` object, install this package:

```bash
    pip install git+https://github.com/reichlab/cladetime.git
```

Once the package is installed, you can instantiate a `CladeTime` object in a Python console
(see some examples below).

#### Work with the latest Nextstrain Sars-Cov-2 sequence metadata and clade assignments

```python
In [1]: from cladetime import CladeTime

In [2]: ct = CladeTime()

# Return a Polars LazyFrame with the sequence metadata.
In [4]: import polars as pl

In [5]: lf = ct.sequence_metadata

# From there, you can use Polars to manipulate the data as needed
In [6]: filtered_sequence_metadata = (
    lf
    .select(["country", "division", "date", "host", "clade_nextstrain"])
    .rename({"clade_nextstrain": "clade", "division": "location"})
    .filter(
        pl.col("country") == "USA",
        pl.col("host") == "Homo sapiens"
    )
).collect()

In [7]: filtered_sequence_metadata.head()
Out[7]:
shape: (5, 5)
┌─────────┬──────────┬────────────┬──────────────┬───────┐
│ country ┆ location ┆ date       ┆ host         ┆ clade │
│ ---     ┆ ---      ┆ ---        ┆ ---          ┆ ---   │
│ str     ┆ str      ┆ str        ┆ str          ┆ str   │
╞═════════╪══════════╪════════════╪══════════════╪═══════╡
│ USA     ┆ Alabama  ┆ 2022-07-07 ┆ Homo sapiens ┆ 22A   │
│ USA     ┆ Arizona  ┆ 2022-07-02 ┆ Homo sapiens ┆ 22B   │
│ USA     ┆ Arizona  ┆ 2022-07-19 ┆ Homo sapiens ┆ 22B   │
│ USA     ┆ Arizona  ┆ 2022-07-15 ┆ Homo sapiens ┆ 22B   │
│ USA     ┆ Arizona  ┆ 2022-07-20 ┆ Homo sapiens ┆ 22B   │
└─────────┴──────────┴────────────┴──────────────┴───────┘

# Pandas users can create a Pandas dataframe with sequence metadata

In [8]: pandas = lf.collect().to_pandas()

# Metadata from the pipeline that produced the above sequence_data
In [9]: ct.ncov_metadata
Out[9]:
{'schema_version': 'v1',
 'nextclade_version': 'nextclade 3.8.2',
 'nextclade_dataset_name': 'SARS-CoV-2',
 'nextclade_dataset_version': '2024-09-25--21-50-30Z',
 'nextclade_tsv_sha256sum': 'fbe579554e925e4dfaf74cfb4e72b52c702e671f0f0374d896f1e30ae4fe5566',
 'metadata_tsv_sha256sum': '5a4fd84a5cd3c4ead9cf730d4df10b8734898c6c3e0cae1c8c0acf432325d22c'}
 ```

 #### Work with point-in-time Nextstrain Sars-Cov-2 sequence metadata and clade assignments

 ```python
In [10]: from cladetime import CladeTime

In [11]: ct = CladeTime(sequence_as_of="2024-08-31", tree_as_of="2024-08-01")

# URL for the corresponding Nextstrain Sars-Cov-2 sequence metadata as it existing on 2024-08-31
In [12]: ct.url_sequence_metadata
Out[12]: 'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/metadata.tsv.zst?versionId=1SZMfjWxXjNy530F6L7MfyflUCbue.JD'

# Metadata for the pipeline run that produced the above file
In [13]: ct.ncov_metadata
Out[13]: {'schema_version': 'v1',
 'nextclade_version': 'nextclade 3.8.2',
 'nextclade_dataset_name': 'SARS-CoV-2',
 'nextclade_dataset_version': '2024-07-17--12-57-03Z',
 'nextclade_tsv_sha256sum': 'fd30f0b258f73fdcf5acefe77937ebe7d88862093bb4aaf3a7e935650ccea060',
 'metadata_tsv_sha256sum': '898451d9750128b4f90253d91cef0092e51965e879536e80aa6598de0fd4af29'}
```


## Local Machine Setup

The sections below provide instructions for setting up this project on your local machine.

**Prerequisites**

Before setting up the project:

- Your machine will need to have an installed version of Python that meets the `requires-python` constraint in [pyproject.toml](pyproject.toml)
- That version of Python should be set as your current Python interpreter (if you don't already have a preferred Python workflow, [pyenv](https://github.com/pyenv/pyenv) is good tool for managing Python versions on your local machine).
- You will need to install two CLI tools used by the pipeline, and ensure they're available in your PATH:
    - [dataformat](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/download-and-install/), to format sequence metadata
    - [Nextclade](https://docs.nextstrain.org/projects/nextclade/en/stable/user/nextclade-cli/installation/index.html), to assign clades to sequences

In addition, if you're planning to make code changes that require adding or removing project dependencies, you'll need `pip-tools` installed on your machine. ([`pipx`](https://github.com/pypa/pipx) is a handy way to install python packages in a way that makes them available all the time, regardless of whatever virtual environment is currently activated.)

**Setup**

Follow the directions below to set this project up on your local machine.

1. Clone this repository and change into the project's high-level directory:

    ```bash
    cd cladetime
    ```

2. Create a Python virtual environment:

    ```bash
    python -m venv .venv
    ```

    **Note:** the resulting virtual environment will use whatever Python interpreter was active when you ran the command

3. Activate the virtual environment:

    ```bash
    source .venv/bin/activate
    ```

    **Note:** the command above is for Unix-based systems. If you're using Windows, the command is:

    ```bash
    .venv\Scripts\activate
    ```

4. Install the project dependencies. The following command can also be used to update dependencies after pulling upstream code changes:

    ```bash

    pip install -r requirements/requirements-dev.txt && pip install -e .
    ```

### Running the test suite

To run the unit tests:

```bash
pytest -k unit
```

To run the full test suite, including an integration test that runs the pipeline end-to-end:

```bash
pytest
```

### Adding new dependencies

At a high-level, this is the process for adding (or removing) a project dependency:

- add/remove the dependency in `pyproject.toml`
- use [`pip-tools`](https://github.com/jazzband/pip-tools) to read the dependencies in `pyproject.toml` and output detailed requirements files

To add a new dependency:

1. Make sure `pip-tools` is installed and available to your virtual environment (if you want `pip-tools` to be available across all virtual environments on your machine, you can install it using [`pipx`](https://pipx.pypa.io/stable/)).

    ```bash
    pip install pip-tools
    ```

2. Add dependency to the `dependencies` section `pyproject.toml` (if it's a dev dependency,
add it to the `dev` section of `[project.optional-dependencies]`).

3. Regenerate the `requirements.txt` file (if you've only added a dev dependency, you can skip this step)
    ```bash
    pip-compile -o requirements/requirements.txt pyproject.toml
    ```

4. Regenerate the `requirements-dev.txt` file (even if you haven't added a dev dependency):
    ```bash
    pip-compile --extra dev -o requirements/requirements-dev.txt pyproject.toml
    ```

## Running the code

Set up the project as described above and make sure the virtual environment is activated. The code that downloads the Genbank
sequences and assigns them to clades is a command-line tool called `assign_clades`.

To see the options and other help information:

```bash
assign_clades --help
```

To download Genbank sequences that have been released since 2024-08-02 and assign clades to them using the SARS-Cov-2 reference tree as it looked on 2024-07-13:

```bash
assign_clades --sequence-released-since-date 2024-08-02 --reference-tree-date 2024-07-13
```
