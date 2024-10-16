:og:description: Cladetime is a Python interface for accessing SARS-CoV-2 sequence and clade data provided by Nextstrain.

===============
 Cladetime
===============

Cladetime is a Python interface for accessing `Nextstrain <https://nextstrain.org/>`_ SARS-CoV-2 sequence and clade data.

.. toctree::
   :titlesonly:
   :hidden:

   Home <self>
   user-guide
   reference/index

Installation
------------

Cladetime can be installed with `pip <https://pip.pypa.io/>`_:

.. code-block:: bash

   $ pip install git+https://github.com/reichlab/cladetime.git


Usage
-----

The CladeTime :any:`CladeTime` class provides a lightweight wrapper around historical and current
SARS-CoV-2 GenBank sequence and sequence metadata created by `nextstrain.org's <https://nextstrain.org/>`_
daily workflow pipeline.

.. code-block:: python

   >>> import polars as pl
   >>> from cladetime import CladeTime

   >>> ct = CladeTime()
   >>> filtered_sequence_metadata = (
   ...    ct.sequence_metadata.select(["country", "division", "date", "host", "clade_nextstrain"])
   ...    .filter(
   ...        pl.col("country") == "USA",
   ...        pl.col("date").is_not_null(),
   ...        pl.col("host") == "Homo sapiens",
   ...    )
   ...    .cast({"date": pl.Date}, strict=False)
   ... )

   >>> filtered_sequence_metadata.head(5).collect()

   shape: (5, 5)
   ┌─────────┬──────────┬────────────┬──────────────┬──────────────────┐
   │ country ┆ division ┆ date       ┆ host         ┆ clade_nextstrain │
   │ ---     ┆ ---      ┆ ---        ┆ ---          ┆ ---              │
   │ str     ┆ str      ┆ date       ┆ str          ┆ str              │
   ╞═════════╪══════════╪════════════╪══════════════╪══════════════════╡
   │ USA     ┆ Alabama  ┆ 2022-07-07 ┆ Homo sapiens ┆ 22A              │
   │ USA     ┆ Arizona  ┆ 2022-07-02 ┆ Homo sapiens ┆ 22B              │
   │ USA     ┆ Arizona  ┆ 2022-07-19 ┆ Homo sapiens ┆ 22B              │
   │ USA     ┆ Arizona  ┆ 2022-07-15 ┆ Homo sapiens ┆ 22B              │
   │ USA     ┆ Arizona  ┆ 2022-07-20 ┆ Homo sapiens ┆ 22B              │
   └─────────┴──────────┴────────────┴──────────────┴──────────────────┘

See the :doc:`user-guide` for more details about working with Cladetime.

The :doc:`reference/index` documentation provides API-level documentation.

