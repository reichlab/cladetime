![cladetime CI status](https://github.com/reichlab/cladetime/actions/workflows/ci.yaml/badge.svg)

# User Guide

Cladetime is a wrapper around Nextstrain's GenBank-based SARS-CoV-2 genome
sequence data and the metadata that describes it. Included with the metadata
are the clades (variants) that each sequence is assigned to.

An advanced feature of Cladetime is the ability to perform custom clade
assignments using past reference trees. For example, you can use the
current set of sequence data and assign clades to it using the reference tree
as it existed three months ago.

Cladetime is designed for use with US-based sequences from Homo sapiens.

## Installation

Cladetime is written in Python and can be installed using pip:

```bash
pip install cladetime
```

## The CladeTime class

Most of Cladetime's features are accessible through the `CladeTime` class,
which accepts two optional parameters:

- `sequence_as_of`: access Nextstrain SARS-CoV-2 sequence data and metadata
files as they existing on this date (defaults to the current UTC datetime)
- `tree_as_of`: the date of the reference tree to use for clade assignments
(defaults to `sequence_as_of`)

> [!IMPORTANT]
> Using `tree_as_of` for custom clade assignments is an advanced feature
> and requires Docker.

```python
>>> from cladetime import CladeTime

# Create a CladeTime object that references the most recent available sequence
# data and metadata from Nextstrain
>>> ct = CladeTime()
```

## Accessing sequence data

Each `CladeTime` object has a link to the full set of Nextstrain's SARS-Cov-2
genomic sequences as they existed on the `sequence_as_of` date. This data
is in .fasta format, and most users won't need to download it directly.

```python
>>> from cladetime import CladeTime
>>> ct = CladeTime()
>>> ct.url_sequence
https://nextstrain-data.s3.amazonaws.com/files/ncov/open/sequences.fasta.xz?versionId=4Sv2PbA1NoEd.V_LOOQSBPkqBpdoj7s_'
```

More interesting to most users will be the [metadata that describes each
sequence](https://docs.nextstrain.org/projects/ncov/en/latest/reference/metadata-fields.html).

The `sequence_metadata` attribute of a `CladeTime` object is a Polars LazyFrame
that points to a copy of Nextstrain's sequence metadata.

You can apply your own filters and transformations to the LazyFrame, but
it's a good idea to start with the built-in `filter_metadata` function that
removes non-US and non-human sequences from the metadata.

A `collect()` operation will return the filtered metadata as an in-memory
Polars DataFrame.

```python
>>> import polars as pl
>>> from cladetime import CladeTime, sequence

>>> ct = CladeTime()
>>> filtered_metadata = sequence.filter_metadata(ct.sequence_metadata)

# Alternately, specify a sequence collection date range to the filter
>>> filtered_metadata = sequence.filter_metadata(
>>>     ct.sequence_metadata,
>>>     collection_min_date = "2024-10-01",collection_max_date ="2024-10-31"
>>> )

>>> metadata_df = filtered_metadata.collect(streaming=True)

# Pandas users can export Polars dataframes
>>> pandas_df = filtered_sequence_metadata.to_pandas()
```

## Past sequence data

Working with past sequence data and metadata is similar to the above examples.
Just pass in a `sequence_as_of` date when creating a `CladeTime` object.

The clades returned as part of the metadata will reflect the reference tree
in use when sequence metadata file was created.

```python
>>> from cladetime import CladeTime

# Create a CladeTime object for any date after May, 2023
>>> ct = CladeTime(sequence_as_of="2024-10-15")
```

## Custom clade assignments

You may want to assign sequence clades using a reference tree from a past date.
This feature is helpful when creating "source of truth" data to evaluate
models that predict clade proportions:

- create a `CladeTime` object using the `tree_as_of` parameter
- filter the sequence metadata to include only the sequences you want to assign
- pass the filtered metadata to the `assign_clades` method

CladeTime's `assign_clades` method returns two Polars LazyFrames:

- `detail`: a linefile of each sequence and its assigned clade
- `summary`: clade counts summarized by `country`, `location`, `date` and `host`

> [!WARNING]
> In addition to requiring Docker, assign_clades is resource-intensive,
> because the process requires downloading a full copy of SARS-CoV-2
> sequence data and then filtering it.
>
> The filtered sequences are then run through Nextclade's CLI for clade
> assignment, another resource-intensive process. We recommend not
> assigning more than 30 days worth of sequence collections at a time.

```python
>>> import polars as pl
>>> from cladetime import CladeTime, sequence

>>> ct = CladeTime(sequence_as_of="2024-11-15", tree_as_of="2024-09-01")
>>> filtered_metadata = sequence.filter_metadata(
>>>     ct.sequence_metadata,
>>>     collection_min_date = "2024-10-01",
>>>     collection_max_date ="2024-10-31"
>>> )
>>> clade_assignments = ct.assign_clades(filtered_metadata)

# Summarized clade assignments
>>> clade_assignments.summary.collect().head()
shape: (5, 6)
┌──────────┬────────────┬──────────────┬──────────────────┬─────────┬───────┐
│ location ┆ date       ┆ host         ┆ clade_nextstrain ┆ country ┆ count │
│ ---      ┆ ---        ┆ ---          ┆ ---              ┆ ---     ┆ ---   │
│ str      ┆ date       ┆ str          ┆ str              ┆ str     ┆ u32   │
╞══════════╪════════════╪══════════════╪══════════════════╪═════════╪═══════╡
│ IL       ┆ 2024-10-28 ┆ Homo sapiens ┆ 24C              ┆ USA     ┆ 1     │
│ IL       ┆ 2024-10-11 ┆ Homo sapiens ┆ 24C              ┆ USA     ┆ 5     │
│ NY       ┆ 2024-10-08 ┆ Homo sapiens ┆ 24B              ┆ USA     ┆ 2     │
│ AZ       ┆ 2024-10-15 ┆ Homo sapiens ┆ 24C              ┆ USA     ┆ 1     │
│ MN       ┆ 2024-10-06 ┆ Homo sapiens ┆ 24A              ┆ USA     ┆ 2     │
└──────────┴────────────┴──────────────┴──────────────────┴─────────┴───────┘

# Detailed clade assignments
>>> clade_assignments.detail.collect().select(
>>>     ["country", "location", "date", "strain", "clade_nextstrain"]
>>>    ).head()
shape: (5, 5)
┌─────────┬──────────┬────────────┬─────────────────────┬──────────────────┐
│ country ┆ location ┆ date       ┆ strain              ┆ clade_nextstrain │
│ ---     ┆ ---      ┆ ---        ┆ ---                 ┆ ---              │
│ str     ┆ str      ┆ date       ┆ str                 ┆ str              │
╞═════════╪══════════╪════════════╪═════════════════════╪══════════════════╡
│ USA     ┆ AZ       ┆ 2024-10-01 ┆ USA/2024CV1711/2024 ┆ 24C              │
│ USA     ┆ AZ       ┆ 2024-10-02 ┆ USA/2024CV1718/2024 ┆ 24C              │
│ USA     ┆ AZ       ┆ 2024-10-04 ┆ USA/2024CV1719/2024 ┆ 24C              │
│ USA     ┆ AZ       ┆ 2024-10-05 ┆ USA/2024CV1721/2024 ┆ 24C              │
│ USA     ┆ AZ       ┆ 2024-10-06 ┆ USA/2024CV1722/2024 ┆ recombinant      │
└─────────┴──────────┴────────────┴─────────────────────┴──────────────────┘
```

## Reproducibility

`CladeTime` objects have an `ncov_metadata` property with information needed to
reproduce the clade assignments in the object's sequence metadata.

In the example below, `ncov_metadata` shows that the
[Nextclade dataset](https://docs.nextstrain.org/projects/nextclade/en/stable/user/datasets.html)
used for clade assignment on 2024-09-22 was `2024-07-17--12-57-03Z`.

Each version of a SARS-CoV-2 Nextclade dataset contains a reference tree
that can be used as an input for clade assignments.

```python
>>> from cladetime import CladeTime
>>> ct = CladeTime(sequence_as_of='2024-09-22')

>>> ct.ncov_metadata.get('nextclade_dataset_name')
'SARS-CoV-2'
>>> ct.ncov_metadata.get('nextclade_dataset_version')
'2024-07-17--12-57-03Z'
```

Access to historical copies of `ncov_metadata` is what allows Cladetime to
access past reference trees for custom clade assignments. Cladetime retrieves
a separate set of `ncov_metadata` for the `tree_as_of` date and uses it to pass
the correct reference tree to the `assign_clades` method.

## Historical Data Access

Cladetime is designed to work with historical data dating back to May 2023. However,
in October 2025, Nextstrain began removing historical `metadata_version.json` files
from their S3 storage, which would prevent access to historical reference tree metadata.

To address this, **Cladetime v2.0.0 introduced automatic fallback** to the
[variant-nowcast-hub](https://github.com/reichlab/variant-nowcast-hub) archives
when Nextstrain S3 data is unavailable. This fallback is completely transparent to users:

1. Cladetime first attempts to retrieve metadata from Nextstrain S3 (fast path)
2. If the historical metadata is not found, Cladetime automatically searches the
   variant-nowcast-hub's `auxiliary-data/modeled-clades/` directory for archived metadata
3. The fallback searches for an exact date match first, then searches backward up to 30 days
   for the nearest prior archive
4. If metadata is found via fallback, a warning is logged but execution continues normally

**Archive availability**: The variant-nowcast-hub has maintained weekly metadata archives
since September 2024. For dates prior to September 2024, Nextstrain S3 may still be the
only source.

**No action required**: The fallback mechanism works automatically. Users don't need to
modify any code or provide additional parameters. If both sources fail, Cladetime will
raise an appropriate error with instructions.

```python
>>> from cladetime import CladeTime

# This works even if Nextstrain S3 lacks historical metadata for 2024-10-09
# Cladetime will automatically fall back to variant-nowcast-hub archives
>>> ct = CladeTime(tree_as_of="2024-10-09")
>>> ct.ncov_metadata.get('nextclade_dataset_version')
'2024-09-25--21-50-30Z'
```

## Acknowledgements

Cladetime uses Genbank-based genome sequence data and metadata published by Nextstrain:

- [Hadfield et al., Nextstrain: real-time tracking of pathogen evolution, Bioinformatics (2018)](https://academic.oup.com/bioinformatics/article/34/23/4121/5001388?login=false)
- [https://nextstrain.org/](https://nextstrain.org/)

Additionally, Cladetime uses the
[Nextclade project](https://docs.nextstrain.org/projects/nextclade/en/stable/)
to assign clades to SARS-CoV-2 genome sequences.

- Aksamentov, I., Roemer, C., Hodcroft, E. B., & Neher, R. A., (2021).
  Nextclade: clade assignment, mutation calling and quality control for viral genomes.
  Journal of Open Source Software, 6(67), 3773, [https://doi.org/10.21105/joss.03773](https://doi.org/10.21105/joss.03773)
- [https://clades.nextstrain.org](https://clades.nextstrain.org)