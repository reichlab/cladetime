:og:description: Cladetime is a Python interface for accessing SARS-CoV-2 sequence and clade data provided by Nextstrain.

===============
 Cladetime
===============

Cladetime is a Python interface for accessing past and present
:external+nextclade:doc:`Nextstrain<index>`-produced SARS-CoV-2 sequence data,
including sequence :external+ncov:doc:`clade<reference/naming_clades>`
assignments. Cladetime can also "time travel" by using prior versions of
:external+nextclade:doc:`reference trees<user/input-files/04-reference-tree>`
to assign clades to sequences.

.. toctree::
   :titlesonly:
   :hidden:

   Home <self>
   User Guide <read-me>
   reference/index

.. sidebar-links::
   :github:

Installation
------------

Cladetime can be installed with `pip <https://pip.pypa.io/>`_:

.. code-block:: bash

   $ pip install cladetime

Detailed documentation
----------------------

- See the :doc:`read-me` for more details about working with Cladetime.
- The :doc:`reference/index` documentation provides API-level documentation.

Usage
-----

Cladetime's :any:`CladeTime` class provides a lightweight wrapper around
historical and current SARS-CoV-2 GenBank sequence and sequence metadata
created by daily Nextstrain
:external+nextstrain:term:`workflows<workflow>`.

See the :doc:`read-me` for examples of creating `CladeTime` objects that
can access past sequence data and use past reference trees to assign clades
to sequences.

.. code-block:: python

   >>> import polars as pl
   >>> from cladetime import CladeTime, sequence

   >>> ct = CladeTime()
   >>> filtered_sequence_metadata = sequence.filter_metadata(
   ...    ct.sequence_metadata
   ... )

   >>> filtered_sequence_metadata.head(5).collect()

   shape: (5, 6)
   ┌───────┬─────────┬────────────┬────────────────────────────┬──────────────┬──────────┐
   │ clade ┆ country ┆ date       ┆ strain                     ┆ host         ┆ location │
   │ ---   ┆ ---     ┆ ---        ┆ ---                        ┆ ---          ┆ ---      │
   │ str   ┆ str     ┆ date       ┆ str                        ┆ str          ┆ str      │
   ╞═══════╪═════════╪════════════╪════════════════════════════╪══════════════╪══════════╡
   │ 22A   ┆ USA     ┆ 2022-07-07 ┆ Alabama/SEARCH-202312/2022 ┆ Homo sapiens ┆ AL       │
   │ 22B   ┆ USA     ┆ 2022-07-02 ┆ Arizona/SEARCH-201153/2022 ┆ Homo sapiens ┆ AZ       │
   │ 22B   ┆ USA     ┆ 2022-07-19 ┆ Arizona/SEARCH-203528/2022 ┆ Homo sapiens ┆ AZ       │
   │ 22B   ┆ USA     ┆ 2022-07-15 ┆ Arizona/SEARCH-203621/2022 ┆ Homo sapiens ┆ AZ       │
   │ 22B   ┆ USA     ┆ 2022-07-20 ┆ Arizona/SEARCH-203625/2022 ┆ Homo sapiens ┆ AZ       │
   └───────┴─────────┴────────────┴────────────────────────────┴──────────────┴──────────┘
