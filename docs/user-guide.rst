User Guide
===========


Finding Nextstrain SARS-CoV-2 sequence data
--------------------------------------------

The primary interface of the cladetime package is
:py:class:`CladeTime<cladetime.CladeTime>`, a class that provides a lightweight
interface to Nextstrain's SARS-CoV-2 sequence data in fasta format.

.. code-block:: python

    >>> from cladetime import CladeTime
    >>> ct = CladeTime()

    # URL to the most recent SARS-CoV-2 sequence file (.fasta)
    >>> ct.url_sequence
    'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/sequences.fasta.zst?versionId=abc'

    # URL to the metadata that describes the sequences in the above file
    >>> ct.url_sequence_metadata
    'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/metadata.tsv.zst?versionId=def'


Working with SARS-CoV-2 sequence metadata
------------------------------------------

The SARS-CoV-2 sequence data published by Nextstrain is accompanied by a
metadata file that describes the sequences in detail. The metadata also
includes a clade assignment for each sequence. Nextstrain assigns clades
based on the most current version of the reference tree.

To download the metadata, use the URL provided by
:py:attr:`url_sequence_metadata<cladetime.CladeTime.url_sequence_metadata>`.

.. code-block:: python

    >>> from cladetime import CladeTime
    >>> ct = CladeTime()
    >>> ct.url_sequence_metadata
   'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/metadata.tsv.zst?versionId=UB179_e2CqBT.LMwCYT12FRab8eymTT.'

Alternately, CladeTime objects have a
:py:attr:`sequence_metadata<cladetime.CladeTime.sequence_metadata>`
property. This property resolves to a Polars LazyFrame for working
with metadata interactively, without downloading the entire file.

.. code-block:: python

    >>> from datetime import datetime
    >>> import polars as pl
    >>> from cladetime import CladeTime
    >>> ct = CladeTime()
    >>> metadata = ct.sequence_metadata  # Returns a Polars LazyFrame

    # From there, use Polars to manipulate the data as needed
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
    ┌────────────┬──────────────────┬─────┐
    │ date       ┆ clade_nextstrain ┆ len │
    │ ---        ┆ ---              ┆ --- │
    │ date       ┆ str              ┆ u32 │
    ╞════════════╪══════════════════╪═════╡
    │ 2024-09-06 ┆ recombinant      ┆ 1   │
    │ 2024-09-17 ┆ 24E              ┆ 181 │
    │ 2024-09-30 ┆ 24F              ┆ 24  │
    │ 2024-09-03 ┆ 24E              ┆ 225 │
    │ 2024-09-11 ┆ 24F              ┆ 24  │
    └────────────┴──────────────────┴─────┘

Note that the collect() function may take several minutes to complete, as the metadata file is large.


Getting historical SARS-CoV-2 sequence metadata
------------------------------------------------

A CladeTime instance created without parameters will reference the most
recent data available from Nextstrain.

To travel back in time and access past sequences and metadata, use the
seq_as_of parameter when instantiating a CladeTime object.

seq_as_of accepts a date string in the format 'YYYY-MM-DD'. Alternately,
you can pass a Python datetime object. Both will be treated as UTC dates/times.
If a date string is specified, Cladetime will convert it to a datetime with
00:00:00 hours:minutes:seconds, meaning that the CladeTime object will
reference the sequence data and metadata available at the start of the day.

.. code-block:: python

    >>> from cladetime import CladeTime
    >>> ct = CladeTime(sequence_as_of='2024-10-15')

    # URL to the SARS-CoV-2 sequence file as of October 15, 2024
    >>> ct.url_sequence
    'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/sequences.fasta.zst?versionId=8Zszokay3LRP5Zec_cviQ8oXkx8cJlwq'

    # URL to the sequence metadata as of October 15, 2024
    >>> ct.url_sequence_metadata
    'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/metadata.tsv.zst?versionId=U4aIlh5HI1XuDLPW7q9WTZad6gXwARqT'


Reference trees
----------------

Using the :py:class:`Tree<cladetime.Tree>` class, Cladetime can also access Nextstrain's
SARS-CoV-2 reference trees from past dates (back to August 1, 2024). This is
useful for assigning sequences to clades based on a specific reference tree.

.. code-block:: python

    >>> from cladetime import CladeTime, Tree

    >>> ct = CladeTime(tree_as_of='2024-09-01')
    >>> ref_tree = Tree(ct)

    # URL to the reference tree as of September 1, 2024
    >>> ref_tree.url
    'https://data.clades.nextstrain.org/v3/nextstrain/sars-cov-2/wuhan-hu-1/orfs/2024-07-17--12-57-03Z/tree.json'

    # The reference tree as of September 1, 2024 (a Python dictionary)
    >>> ref_tree.tree.keys()
    dict_keys(['version', 'meta', 'tree', 'root_sequence'])

    >>> print(ref_tree.tree['meta']['title'], ref_tree.tree['meta']['updated'])
    SARS-CoV-2 phylogeny 2024-07-17


Clade assignments with past sequences and reference trees
----------------------------------------------------------

Coming soon!


Reproducibility
----------------

CladeTime objects have a :py:attr:`ncov_metadata<cladetime.CladeTime.ncov_metadata>` attribute
with pipeline metadata from the Nexstrain process that produced the
sequence and sequence metadata files.

This pipeline metadata is available from 2024-08-01 onwards.

.. code-block:: python

    >>> from cladetime import CladeTime
    >>> ct = CladeTime(sequence_as_of='2024-09-22')

    >>> ct.ncov_metadata
    {
    "schema_version": "v1",
    "nextclade_version": "nextclade 3.8.2",
    "nextclade_dataset_name": "SARS-CoV-2",
    "nextclade_dataset_version": "2024-07-17--12-57-03Z",
    "nextclade_tsv_sha256sum": "482cad6735e6a0ca6da377e3bd7a25195e9ff3865babd76eb77cd5e00e13704a",
    "metadata_tsv_sha256sum": "88831928d7aef30854599ff35ac20aab15d4f5f53d772c5142f1ea79b619f137",
    "nextclade_dataset_name_full": "nextstrain/sars-cov-2/wuhan-hu-1/orfs",
    }
