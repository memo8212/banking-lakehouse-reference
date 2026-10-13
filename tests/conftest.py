import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse.cli import make_spark
    s = make_spark("bank_lakehouse_test")
    yield s
    s.stop()
