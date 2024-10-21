"""Custom exceptions for cladetime."""


class Error(Exception):
    """Base class for exceptions raised by cladetime."""


class CladeTimeInvalidDateError(Error):
    """Raised when an invalid date string is passed to CladeTime."""


class CladeTimeInvalidURLError(Error):
    """Raised when CladeTime encounters an invalid URL."""


class CladeTimeFutureDateWarning(Warning):
    """Raised when CladeTime as_of date is in the future."""


class TreeNotAvailableError(Error):
    """Raised when CladeTime cannot retrieve a reference tree for tree_as_of."""
