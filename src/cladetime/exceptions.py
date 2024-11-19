"""Custom exceptions for Cladetime."""


class Error(Exception):
    """Base class for exceptions raised by cladetime."""


class CladeTimeInvalidURLError(Error):
    """Raised when CladeTime encounters an invalid URL."""


class CladeTimeDateWarning(Warning):
    """Raised when CladeTime as_of date is in the future."""


class CladeTimeSequenceWarning(Warning):
    """Raised when filtered sequence metadata is empty."""


class NextcladeNotAvailableError(Error):
    """Raised when Nextclade CLI is not available."""


class TreeNotAvailableError(Error):
    """Raised when CladeTime cannot retrieve a reference tree for tree_as_of."""
