"""
Tests dal.local_disk.py module
"""
import os
import tempfile
from unittest import TestCase

from job1.dal.local_disk import save_to_disk


class SaveToDiskTestCase(TestCase):
    """
    Test dal.local_disk.save_to_disk function.
    """

    def test_save_to_disk(self):
        """
        Test whether json data is saved to disk.
        """
        with tempfile.TemporaryDirectory() as dir:
            data = [{"foo": "bar"}]
            path = dir + "/raw/data.json"

            assert not os.path.exists(path)
            save_to_disk(data, path)

            assert os.path.exists(path)
            with open(path) as fp:
                import json

                assert json.load(fp) == data
