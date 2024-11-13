# Cladetime Test Data

This directory contains test files used by CladeTime's test suite.

* `test_metadata.tsv` was used to test `get_clade_list` before that functionality moved to variant-nowcast-hub
* `metadata.tsv.xz` and `metadata.tsv.xz` are used to test setting CladeTime's sequence_metadata property.
* `test_sequence.xz` is used to test the sequence filter function
* `test_sequences.fasta`, `test_sequences.fasta`, and `test_nexclade_dataset.zip` are used in Nextclade integration tests
* `test_sequences_evolving.fasta` is used to test clade assignments with prior reference trees
  * it contains 3 sequence strains with clade assignments that changed between 2024-08-02 and 2024-11-07
  * differing clade assignments were determined by comparing the 2024-08-02 and 2024-11-07 versions of Nexstrain's sequence metadata
  * `USA/VA-CDC-LC1109961/2024` is assigned to `24C` as of 2024-08-02 and `24E` as of 2024-11-07
  * `USA/FL-CDC-LC1109983/2024` is assigned to `24B` as of 2024-08-02 and `24G` as of 2024-11-07
  * `USA/MD-CDC-LC1110088/2024` is assigned to `24B` as of 2024-08-02 and `24G` as of 2024-11-07
