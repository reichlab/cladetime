# Cladetime

Cladetime is a wrapper around Nextstrain datasets and tools for downloading and working with SARS-CoV-2 virus genome sequence data at
a specific point in time.

## Usage

This library contains two types of components:

1. Scripts and CLI tools for use in database pipelines required by the [Variant Nowcast Hub](https://github.com/reichlab/variant-nowcast-hub) (these are in development and not documented here).

2. Python classes for interactively working with SARS-CoV-2 sequence data, metadata, and clade assignments.

## Getting started with Cladetime

To use `cladetime` interactively, install the package from GitHub:

```bash
pip install git+https://github.com/reichlab/cladetime.git
```

Below are a few examples of using Cladetime in a Python interpreter.
See the [project documentation](https://cladetime.readthedocs.io) for more details.

### Time traveling with Cladetime

Cladetime knows where to find past versions of Nextstrain's SARS-CoV-2 sequence data and metadata files.
These are [intermediate files produced by Nextstrain's daily workflow](https://docs.nextstrain.org/projects/ncov/en/latest/reference/remote_inputs.html#remote-inputs-open-files). Cladetime uses the full set of open Genbank data.

```python
>>> from cladetime import CladeTime

# Create a CladeTime object for any date after May 2023
# (for the most recent data, remove the `sequence_as_of` parameter)
>>> ct = CladeTime(sequence_as_of="2024-10-15")

# Based on the sequence_as_of date (above), Cladetime provides URLs to the corresponding
# SARS-CoV-2 past sequence data and metadata files
>>> ct.url_sequence
'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/sequences.fasta.zst?versionId=8Zszokay3LRP5Zec_cviQ8oXkx8cJlwq'
>>> ct.url_sequence_metadata
'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/metadata.tsv.zst?versionId=U4aIlh5HI1XuDLPW7q9WTZad6gXwARqT'
```

### Interacting with sequence metadata

The [metadata provided by Nextstrain](https://docs.nextstrain.org/projects/ncov/en/latest/reference/metadata-fields.html)
contains descriptive information about SARS-CoV-2 sequences. Cladetime provides a Polars-based interface to work with
this metadata directly, without downloading it first.

```python
>>> from datetime import datetime
>>> import polars as pl
>>> from cladetime import CladeTime

>>> ct = CladeTime()
>>> metadata = ct.sequence_metadata  # Returns a Polars LazyFrame

# From there, you can use Polars to manipulate the data as needed
>>> filtered_sequence_metadata = (
...     metadata
...     .select(["country", "division", "date", "host", "clade_nextstrain"])
...     .rename({"division": "location"})
...     .cast({"date": pl.Date}, strict=False)
...     .filter(
...         pl.col("country") == "USA",
...         pl.col("date") >= pl.lit(datetime(2024, 9, 1)),
...         pl.col("date") < pl.lit(datetime(2024, 11, 1)),
...     )
...     .group_by(["date", "clade_nextstrain"]).len()
... ).collect(streaming=True)

>>> filtered_sequence_metadata.head(5)
shape: (5, 3)
┌────────────┬──────────────────┬─────┐
│ date       ┆ clade_nextstrain ┆ len │
│ ---        ┆ ---              ┆ --- │
│ date       ┆ str              ┆ u32 │
╞════════════╪══════════════════╪═════╡
│ 2024-09-05 ┆ 24B              ┆ 42  │
│ 2024-09-26 ┆ 24E              ┆ 73  │
│ 2024-09-03 ┆ 24A              ┆ 100 │
│ 2024-09-24 ┆ 24F              ┆ 17  │
│ 2024-09-25 ┆ 24B              ┆ 12  │
└────────────┴──────────────────┴─────┘

# Pandas users can export Polars dataframes
>>> pandas_df = filtered_sequence_metadata.to_pandas()
```

### Reference trees and clade assignments

Cladetime also allows you to access SARS-CoV-2 reference trees from past dates (back to August 1, 2024).
This is useful for assigning sequences to clades based on a specific reference tree.

```python
>>> from cladetime import CladeTime, Tree

>>> ct = CladeTime(tree_as_of="2024-09-01")
>>> ref_tree = Tree(ct)

# The tree object provides a URL to the reference tree as
# it existed on the "tree_as_of" date
>>> ref_tree.url
'https://data.clades.nextstrain.org/v3/nextstrain/sars-cov-2/wuhan-hu-1/orfs/2024-07-17--12-57-03Z/tree.json'

# It also contains the tree itself, in the form of a Python dictionary
>>> ref_tree.tree.keys()
dict_keys(['version', 'meta', 'tree', 'root_sequence'])

>>> print(ref_tree.tree['meta']['title'], ref_tree.tree['meta']['updated'])
SARS-CoV-2 phylogeny 2024-07-17
```

### Clade assignments with past sequences and reference trees

Coming soon!
