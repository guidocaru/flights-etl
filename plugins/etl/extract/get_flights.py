import requests
from typing import Literal, Any
from plugins.utils.dates import today, yesterday
import os
import time


API_KEY = os.environ.get("API_KEY")
API_URL = os.environ.get("API_URL")


def get_flights(**context: Any) -> None:
    """
    Fetches both departure and arrival flights for the Ezeiza (SAEZ) airport from yesterday to today, combining both departures and arrivals.
    """

    airport_code = "SAEZ"

    arrivals = fetch_flights(
        airport=airport_code,
        start_date=yesterday(),
        end_date=today(),
        category="arrivals",
    )

    departures = fetch_flights(
        airport=airport_code,
        start_date=yesterday(),
        end_date=today(),
        category="departures",
    )

    fetched_flights = departures + arrivals
    context["ti"].xcom_push(key="fetched_flights", value=fetched_flights)


def fetch_flights(
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

    headers = {"x-apikey": API_KEY}
    endpoint = f"/airports/{airport}/flights/{category}"
    params = {"start": start_date, "end": end_date}
    flights = []

    while True:

        try:
            response = requests.get(API_URL + endpoint, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print("Error requesting the API: ", e)
            break

        flights.extend(data.get(category, []))

        links = data.get("links")

        if not links:
            break

        endpoint = links.get("next")
        params = {}

        # To avoid too many requests
        time.sleep(5)

    return flights
