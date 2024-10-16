import requests
import json
from typing import Literal
import os
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")

headers = {"x-apikey": API_KEY}


def get_flights(
    airport: str,
    start_date: str,
    end_date: str,
    category: Literal["departures", "arrivals"],
) -> list[str]:

    endpoint = f"/airports/{airport}/flights/{category}"
    params = {"start": start_date, "end": end_date}

    flights = []

    while True:

        response = requests.get(API_URL + endpoint, params=params, headers=headers)

        if response.status_code != 200:
            raise ValueError("error", response.status_code)

        data = response.json()
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
