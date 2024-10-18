import requests
import json
from typing import Literal
from config.config import ENV

API_KEY = ENV["API_KEY"]
API_URL = ENV["API_URL"]

headers = {"x-apikey": API_KEY}


def get_flights(
    airport: str,
    start_date: str,
    end_date: str,
    category: Literal["departures", "arrivals"],
) -> list[str]:
    """
    Fetches a list of flights for a given airport, date range, and category (departures or arrivals).

    Args:
        airport (str): The airport code.
        start_date (str): The start date for fetching flights.
        end_date (str): The end date for fetching flights.
        category (Literal["departures", "arrivals"]): The flight category, either "departures" or "arrivals".

    Returns:
        List[str]: A list of fetched flights information.
    """

    endpoint = f"/airports/{airport}/flights/{category}"
    params = {"start": start_date, "end": end_date}
    flights = []

    while True:

        try:
            response = requests.get(API_URL + endpoint, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print("Error requesting the API", e)
            break

        flights.extend(data.get(category, []))

        links = data.get("links")

        if not links:
            break

        endpoint = links.get("next")
        params = {}

    return flights


def get_flights_mock(**context) -> list[str]:
    data = "/opt/airflow/dags/api/mock_flights.json"
    with open(data, "r") as json_file:
        data = json.load(json_file)
        context["ti"].xcom_push(key="my_key", value=data)
