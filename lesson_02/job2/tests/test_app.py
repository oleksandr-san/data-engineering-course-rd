"""
Tests for app.py
"""
from unittest import TestCase, mock

import job2.app as main


class MainFunctionTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch("job2.app.process_stg")
    def test_return_400_stg_dir_param_missed(self, mock: mock.MagicMock):
        """
        Raise 400 HTTP code when no 'stg_dir' param
        """
        resp = self.client.post(
            "/",
            json={
                "raw_dir": "/foo/bar/",
                # no 'stg_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_raw_dir_param_missed(self):
        """
        Raise 400 HTTP code when no 'stg_dir' param
        """
        resp = self.client.post(
            "/",
            json={
                "stg_dir": "1970-01-01",
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch("job2.app.process_stg")
    def test_process_stg(self, process_stg: mock.MagicMock):
        """
        Test whether api.get_sales is called with proper params
        """
        fake_stg_dir = "/stg/bar/"
        fake_raw_dir = "/foo/bar/"
        self.client.post(
            "/",
            json={
                "stg_dir": fake_stg_dir,
                "raw_dir": fake_raw_dir,
            },
        )

        process_stg.assert_called_with(
            stg_dir=fake_stg_dir,
            raw_dir=fake_raw_dir,
        )

    @mock.patch("job2.app.process_stg")
    def test_return_201_when_all_is_ok(self, get_sales_mock: mock.MagicMock):
        fake_stg_dir = "/stg/bar/"
        fake_raw_dir = "/foo/bar/"
        resp = self.client.post(
            "/",
            json={
                "stg_dir": fake_stg_dir,
                "raw_dir": fake_raw_dir,
            },
        )
        assert resp.status_code == 201
