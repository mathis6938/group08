"""Test stand-in for `findspark`.

On the VM, `findspark.init()` points Python to the Spark installation of /opt/spark (as in the labs).
In the tests, PySpark is installed with pip, so there is nothing to find.
"""


def init(*args, **kwargs):
    return None
