# Changelog

All notable changes to Cladetime are documented here. Cladetime uses
[Semantic Versioning](https://semver.org/).

## [unreleased]

### Changed

- **BREAKING**: Updated minimum `sequence_as_of` date from 2023-05-01 to **2025-09-29** due to Nextstrain's ~7-week S3 retention policy
- **BREAKING**: CladeTime now raises `CladeTimeDataUnavailableError` for dates outside data availability windows instead of silently defaulting to current date
- **BREAKING**: Minimum `tree_as_of` date remains 2024-10-09 (via variant-nowcast-hub archives), but now enforced with error instead of warning
- Updated configuration constant `nextstrain_min_seq_date` to reflect new data availability constraints

### Added

- New `CladeTimeDataUnavailableError` exception for dates outside Nextstrain S3 data retention window
- Comprehensive negative tests verifying errors are raised for unavailable dates
- Data availability constraints section in README documenting historical data limitations
- Reference to GitHub issue #185 in error messages and documentation for tracking infrastructure changes

### Removed

- Removed `test_cladetime_assign_clades_historical` test that relied on unavailable historical data (2024-10-30)
- See [GitHub issue #185](https://github.com/reichlab/cladetime/issues/185) for discussion of restoring historical test coverage

### Note

These changes reflect Nextstrain's October 2025 implementation of a ~7-week retention policy for S3 versioned objects. Historical data beyond this window has been permanently deleted. **This limitation may change as Nextstrain's infrastructure evolves.** Users requiring access to historical data should consider archiving datasets locally or using alternative data sources.

## 2.0.0

### Added

- Automatic fallback to variant-nowcast-hub archives when Nextstrain S3 historical metadata is unavailable
- New `_get_metadata_from_hub()` function in `cladetime/util/reference.py` to retrieve metadata from variant-nowcast-hub GitHub archives
- Support for historical metadata access dating back to September 2024 via variant-nowcast-hub archives
- Comprehensive test coverage for fallback mechanism with 5 new test cases in `tests/unit/util/test_reference.py`

### Changed

- `_get_ncov_metadata()` now accepts optional `as_of_date` parameter to enable fallback support
- `_get_ncov_metadata()` logic simplified to eliminate code duplication and improve clarity (thanks @nickreich for the review feedback)
- `Tree` class now catches `ValueError` from `_get_s3_object_url()` and triggers fallback when metadata is missing
- `CladeTime` class now handles missing S3 metadata gracefully with automatic fallback
- **BREAKING**: Test infrastructure updated with new `mock_s3_sequence_data()` and `patch_s3_for_tests()` fixtures to handle Nextstrain's October 2025 S3 cleanup
- All integration and unit tests now use `patch_s3_for_tests` fixture to mock S3 calls

### Fixed

- CladeTime no longer fails when accessing historical dates after Nextstrain's October 2025 cleanup of S3 metadata files
- Tests now pass consistently regardless of Nextstrain S3 historical data availability
- Proper error handling and logging when both S3 and fallback sources are unavailable

## 0.3.0

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