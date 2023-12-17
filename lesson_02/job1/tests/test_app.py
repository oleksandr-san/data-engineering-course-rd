"""
Tests for app.py
"""
from unittest import TestCase, mock

import job1.app as main


class MainFunctionTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch("job1.app.save_sales_to_local_disk")
    def test_return_400_date_param_missed(self, mock: mock.MagicMock):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/",
                # no 'date' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_raw_dir_param_missed(self):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            "/",
            json={
                "date": "1970-01-01",
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch("job1.app.save_sales_to_local_disk")
    def test_save_sales_to_local_disk(
        self, save_sales_to_local_disk_mock: mock.MagicMock
    ):
        """
        Test whether api.get_sales is called with proper params
        """
        fake_date = "1970-01-01"
        fake_raw_dir = "/foo/bar/"
        self.client.post(
            "/",
            json={
                "date": fake_date,
                "raw_dir": fake_raw_dir,
            },
        )

        save_sales_to_local_disk_mock.assert_called_with(
            date=fake_date,
            raw_dir=fake_raw_dir,
        )

    @mock.patch("job1.app.save_sales_to_local_disk")
    def test_return_201_when_all_is_ok(self, get_sales_mock: mock.MagicMock):
        fake_date = "1970-01-01"
        fake_raw_dir = "/foo/bar/"
        resp = self.client.post(
            "/",
            json={
                "date": fake_date,
                "raw_dir": fake_raw_dir,
            },
        )
        assert resp.status_code == 201
