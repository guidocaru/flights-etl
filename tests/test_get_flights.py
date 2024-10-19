import unittest
from unittest.mock import patch, MagicMock
from dags.extract.get_flights import get_flights
import requests


class TestGetFlights(unittest.TestCase):
    @patch("requests.get")
    def test_get_flights_success(self, mock_get):

        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = {
            "departures": ["Flight 1", "Flight 2"],
            "links": {"next": "/airports/SAEZ/flights/departures?page=2"},
        }
        mock_response_2 = MagicMock()
        mock_response_2.json.return_value = {"departures": ["Flight 3"], "links": {}}

        mock_get.side_effect = [mock_response_1, mock_response_2]

        flights = get_flights("SAEZ", "2023-10-01", "2023-10-02", "departures")

        self.assertEqual(flights, ["Flight 1", "Flight 2", "Flight 3"])
        self.assertEqual(mock_get.call_count, 2)

    @patch("requests.get")
    def test_get_flights_api_error(self, mock_get):

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "API error"
        )
        mock_get.return_value = mock_response

        flights = get_flights("SAEZ", "2023-10-01", "2023-10-02", "departures")

        self.assertEqual(flights, [])
        mock_get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
