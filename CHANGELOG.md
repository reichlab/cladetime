# Changelog

All notable changes to Cladetime are documented here. Cladetime uses
[Semantic Versioning](https://semver.org/).

## [unreleased]

### Added

- Cladetime now has a CHANGELOG
- Acknowledgements section in the [README](README.md)

## 0.3.0

### Changed

- Performance improvement: use [biobear](https://github.com/wheretrue/biobear) as .fasta file reader for
  ZSTD-compressed sequence data
- `sequence_as_of` and `tree_as_of` timestamps now default to 23:59:59 UTC instead of 00:00:00 UTC

## 0.2.4

### Added

- Publish Cladetime to PyPI

### Changed

- Make the Clade class public

## 0.2.3

### Added

- [Contributing guidelines](CONTRIBUTING.md)
- [Cladetime package documentation](https://cladetime.readthedocs.io/)
- Support for demo mode that uses Nextstrain's 100k sample instead of an entire SARS-CoV-2 sequence dataset
- New `CladeTime.assign_clades` method that assigns clades to SARS-CoV-2 sequences using a point-in-time reference tree
- New `nextclade_dataset_name` attribute in `CladeTime.ncov_metadata`
- Warning message when Docker is not detected during Cladetime initialization

### Changed

- Package renamed to `cladetime`

### Fixed

- Output clade assignments as .tsv instead of .csv
- Fix UTC timezone bug when setting `CladeTime.sequence_as_of` and `CladeTime.tree_as_of`

### Removed

- Cladetime CLI removed in favor of programmatic usage
- The `get_clade_list.py` functionality has moved to the
  [`variant-nowcast-hub`](https://github.com/reichlab/variant-nowcast-hub/blob/main/src/get_clades_to_model.py)