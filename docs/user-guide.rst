===============
User Guide
===============



Finding Nextstrain SARS-CoV-2 sequences and sequence metadata
--------------------------------------------------------------

Cladetime provides a CladeTime class that provides a lightweight interface to nextstrain.org files.

.. code-block:: python

    >>> from cladetime import CladeTime

    # Instantiating a CladeTime object with no parameters will use the
    # latest available data from nextstrain.org.
    >>> ct = CladeTime()

    # URL to the most recent SARS-CoV-2 sequence file (.fasta)
    >>> ct.url_sequence
    https://nextstrain-data.s3.amazonaws.com/files/ncov/open/sequences.fasta.zst?versionId=d66Hn1T0eFMAg8osEh8Yrod.QEUBRxvu'

    # URL to the metadata that describes the sequences in the above file
    >>> ct.url.sequence_metadata
    'https://nextstrain-data.s3.amazonaws.com/files/ncov/open/metadata.tsv.zst?versionId=JTXXFlKyyvt9AerxKMwoZflhFYQFrDek'

    # Metadata about the nextstrain data pipeline that created generated the sequence file and its metadata
    >>> ct.ncov_metadata
    {'schema_version': 'v1',
    'nextclade_version': 'nextclade 3.8.2',
    'nextclade_dataset_name': 'SARS-CoV-2',
    'nextclade_dataset_version': '2024-09-25--21-50-30Z',
    'nextclade_tsv_sha256sum': '5b0f2b64bfe694a3c96bd5a116de8fae23b144bfd3d22da774d4bfe9a84399c3',
    'metadata_tsv_sha256sum': '1dc6a4204039e5c69eed84583faf75bbec1629e531dc99aab5bd566d3fb28295'}


Working with SARS-CoV-2 sequence metadata
------------------------------------------

The CladeTime class also provides a Polars LazyFrame object that points to the Nextstrain's sequence metadata file.
This file is in .tsv format and contains information about the sequences, such as their collection date,
host, and location.

The metadata also includes a clade assignment for each sequence. Nextstrain assigns clades based on a reference tree,
and the reference tree varies over time.

.. code-block:: python

    >>> import polars as pl
    >>> from cladetime import CladeTime

    >>> ct = CladeTime()

    # ct contains a Polars LazyFrame that references the sequence metadata .tsv file on AWS S3
    >>> lf = ct.sequence_metadata
    >>> lf
    <LazyFrame at 0x105341190>


Getting historical SARS-CoV-2 sequence metadata
------------------------------------------------

A CladeTime instance created without parameters will reference the most
recent data available from Nextstrain.

To access sequence metadata at a specific point in time, pass a date string
in the format 'YYYY-MM-DD' to the CladeTime constructor. Alternatively, you can pass
a Python datetime object. Both will be treated as UTC dates/times. If a date string
is specified, the datetime will be set to 00:00:00 hours:minutes:seconds on that
date, meaning that the CladeTime object will retrieve the sequence metadata that
was available at the start of that day.

.. code-block:: python

    >>> from cladetime import CladeTime

    >>> ct = CladeTime(sequence_as_of="2024-08-02")

    # ct operations now reference the version of the sequence metadata
    # that was available at midnight UTC on August 2, 2024.
    >>> ct.sequence_metadata \
    ...     .cast({"date": pl.Date}, strict=False) \
    ...     .select(pl.max("date")).collect()

    shape: (1, 1)
    ┌────────────┐
    │ date       │
    │ ---        │
    │ date       │
    ╞════════════╡
    │ 2024-07-23 │
    └────────────┘

