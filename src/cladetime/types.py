"""Type aliases for this package."""

from enum import StrEnum


class StateFormat(StrEnum):
    """Options for formatting state names in sequence metadata"""

    ABBR = "abbr"
    """Format states as two-letter abbreviations"""
    NAME = "name"
    """Format states as full names"""
    FIPS = "fips"
    """Format states as FIPS codes"""
