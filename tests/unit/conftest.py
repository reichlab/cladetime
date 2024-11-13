from pathlib import Path

import pytest


@pytest.fixture
def test_file_path() -> Path:
    """
    Return path to the unit test files.
    """
    test_file_path = Path(__file__).parents[1].joinpath("data")
    return test_file_path
