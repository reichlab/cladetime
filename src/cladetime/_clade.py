from dataclasses import dataclass

import polars as pl


@dataclass
class Clade:
    """Holds detailed and summarized information about clade assignments."""

    meta: dict
    detail: pl.LazyFrame
    summary: pl.LazyFrame
