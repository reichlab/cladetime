import os
from dataclasses import InitVar, asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint


@dataclass
class Config:
    data_path_root: InitVar[str] = Path(".")
    now = datetime.now()
    run_time = now.strftime("%Y%m%dT%H%M%S")

    # Nextstrain sequence data files in their current format are published back to 2023-05-01
    nextstrain_min_seq_date: datetime = datetime(2023, 5, 1).replace(tzinfo=timezone.utc)
    # Nextstrain ncov pipeline metadata began publishing on 2024-08-01
    nextstrain_min_ncov_metadata_date: datetime = datetime(2024, 8, 1, 1, 26, 29, tzinfo=timezone.utc)
    nextstrain_ncov_bucket = "nextstrain-data"
    nextstrain_ncov_metadata_key = "files/ncov/open/metadata_version.json"
    nextstrain_genome_metadata_key = "files/ncov/open/metadata.tsv.zst"
    nextstrain_genome_sequence_key = "files/ncov/open/sequences.fasta.zst"
    nextclade_data_url = "https://data.clades.nextstrain.org"
    nextclade_data_url_version = "v3"
    nextclade_base_url: str = "https://nextstrain.org/nextclade/sars-cov-2"
    nextclade_input_tree_name: str = "tree.json"
    clade_assignment_warning_threshold: int = 10000
    # standard metadata fields for Nextstrain ncov pipeline (i.e., excludes clade assignments)
    # https://docs.nextstrain.org/projects/ncov/en/latest/reference/metadata-fields.html
    nextstrain_standard_metadata_fields = [
        "strain",
        "virus",
        "gisaid_epi_isl",
        "genbank_accession",
        "date",
        "region",
        "country",
        "division",
        "location",
        "region_exposure",
        "country_exposure",
        "division_exposure",
        "segment",
        "length",
        "host",
        "age",
        "sex",
        "originating_lab",
        "submitting_lab",
        "authors",
        "url",
        "title",
        "date_submitted",
    ]

    def __post_init__(
        self,
        data_path_root: str | None,
    ):
        if data_path_root:
            self.data_path = Path(data_path_root)
        else:
            self.data_path = Path(".").home() / "covid_variant" / self.run_time

        # For demo purposes, use Nextstrain's 100k sample dataset
        if os.environ.get("CLADETIME_DEMO") == "true":
            self.nextstrain_genome_metadata_key = "files/ncov/open/100k/metadata.tsv.xz"
            self.nextstrain_genome_sequence_key = "files/ncov/open/100k/sequences.fasta.xz"

    def __repr__(self):
        return str(pprint(asdict(self)))
