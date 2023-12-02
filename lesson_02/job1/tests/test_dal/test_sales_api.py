"""
Tests sales_api.py module.
"""
from unittest import TestCase, mock

import requests_mock
from requests import Response

from job1.dal.sales_api import API_URL, PAGES_COUNT, get_sales


class GetSalesTestCase(TestCase):
    """
    Test sales_api.get_sales function.
    """

    @requests_mock.Mocker()
    @mock.patch("job1.dal.sales_api.AUTH_TOKEN", "fake_token")
    def test_get_sales(self, m):
        """
        Test whether sales are retrieved from API.
        """
        m.register_uri("GET", API_URL + "sales", json=[{"foo": "bar"}])
        data = get_sales("1970-01-01")
        self.assertEqual(data, [{"foo": "bar"}] * PAGES_COUNT)
