from dataclasses import dataclass

import polars as pl


@dataclass
class Clade:
    """Holds detailed and summarized information about clade assignments.

    Attributes
    ----------
    meta : dict
        Metadata about the Nextclade dataset and CLI version used to generate
        the clade assignment information in `detail` and `summary`.
    detail : :external+polars:std:doc:`polars.LazyFrame<reference/lazyframe/index>`
        A LazyFrame with a row for each item in the `sequence_meteadata`
        LazyFrame passed to
        :py:meth:`cladetime.CladeTime.assign_clades`.
    summary : :external+polars:std:doc:`polars.LazyFrame<reference/lazyframe/index>`
        A LazyFrame that summarizes clade counts by country, location,
        species, and sequence collection date.
    """

    meta: dict
    detail: pl.LazyFrame
    summary: pl.LazyFrame
