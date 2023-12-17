"""
Tests dal.local_disk.py module
"""
import os
import tempfile
from unittest import TestCase

from job2.dal.local_disk import load_avro, save_avro


class SaveToDiskTestCase(TestCase):
    """
    Test dal.local_disk.save_avro function.
    """

    def test_save_avro(self):
        """
        Test whether json data is saved to disk.
        """
        with tempfile.TemporaryDirectory() as dir:
            data = [
                {
                    "client": "Amanda Padilla",
                    "purchase_date": "2022-08-09",
                    "product": "Phone",
                    "price": 532,
                },
                {
                    "client": "Deanna Castillo",
                    "purchase_date": "2022-08-09",
                    "product": "coffee machine",
                    "price": 1802,
                },
                {
                    "client": "Brandy Watson",
                    "purchase_date": "2022-08-09",
                    "product": "Phone",
                    "price": 1373,
                },
            ]
            path = dir + "/raw/data.avro"

            assert not os.path.exists(path)
            save_avro(data, path)

            assert os.path.exists(path)
            assert load_avro(path) == data
