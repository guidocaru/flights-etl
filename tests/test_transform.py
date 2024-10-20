import pytest
import pandas as pd
from dags.transform.transform import (
    create_df,
    create_flights_df,
    create_airports_df,
    create_dates_df,
)


sample_data = [
    {
        "ident": "ARG1545",
        "ident_icao": "ARG1545",
        "ident_iata": "AR1545",
        "actual_runway_off": "05",
        "actual_runway_on": "11",
        "fa_flight_id": "ARG1545-1728428115-airline-694p",
        "operator": "ARG",
        "operator_icao": "ARG",
        "operator_iata": "AR",
        "flight_number": "1545",
        "registration": "LV-CHO",
        "atc_ident": None,
        "inbound_fa_flight_id": "ARG1544-1728420081-airline-350p",
        "codeshares": ["AMX7456", "AFR3985", "GLO3234"],
        "codeshares_iata": ["AM7456", "AF3985", "G33234"],
        "blocked": False,
        "diverted": False,
        "cancelled": False,
        "position_only": False,
        "origin": {
            "code": "SACO",
            "code_icao": "SACO",
            "code_iata": "COR",
            "code_lid": None,
            "timezone": "America/Argentina/Cordoba",
            "name": "Ingeniero Ambrosio L.V. Taravella Int'l",
            "city": "Cordoba",
            "airport_info_url": "/airports/SACO",
        },
        "destination": {
            "code": "SAEZ",
            "code_icao": "SAEZ",
            "code_iata": "EZE",
            "code_lid": None,
            "timezone": "America/Argentina/Buenos_Aires",
            "name": "Ministro Pistarini Int'l",
            "city": "Ezeiza",
            "airport_info_url": "/airports/SAEZ",
        },
        "departure_delay": -480,
        "arrival_delay": -480,
        "filed_ete": 3600,
        "scheduled_out": "2024-10-10T22:50:00Z",
        "estimated_out": "2024-10-10T22:50:00Z",
        "actual_out": "2024-10-10T22:42:00Z",
        "scheduled_off": "2024-10-10T23:00:00Z",
        "estimated_off": "2024-10-10T22:52:00Z",
        "actual_off": "2024-10-10T22:52:00Z",
        "scheduled_on": "2024-10-11T00:00:00Z",
        "estimated_on": "2024-10-10T23:56:00Z",
        "actual_on": "2024-10-10T23:56:00Z",
        "scheduled_in": "2024-10-11T00:10:00Z",
        "estimated_in": "2024-10-10T23:52:00Z",
        "actual_in": "2024-10-11T00:02:00Z",
        "progress_percent": 100,
        "status": "Arrived / Gate Arrival",
        "aircraft_type": "E190 ",
        "route_distance": 408,
        "filed_airspeed": 355,
        "filed_altitude": None,
        "route": None,
        "baggage_claim": "11",
        "seats_cabin_business": None,
        "seats_cabin_coach": None,
        "seats_cabin_first": None,
        "gate_origin": "5",
        "gate_destination": None,
        "terminal_origin": None,
        "terminal_destination": "DA",
        "type": "Airline",
    },
]


@pytest.fixture
def fetched_df():
    return create_df(sample_data)


def test_create_df(fetched_df):
    assert isinstance(fetched_df, pd.DataFrame)
    assert not fetched_df.empty
    assert "flight_id" in fetched_df.columns
    assert fetched_df["flight_id"].iloc[0] == "ARG1545-1728428115-airline-694p"


def test_create_flights_df(fetched_df):
    flights_df = create_flights_df(fetched_df)
    assert isinstance(flights_df, pd.DataFrame)
    assert "flight_id" in flights_df.columns
    assert "operator_id" in flights_df.columns
    assert flights_df.shape[0] == fetched_df.shape[0]


def test_create_airports_df(fetched_df):
    airports_df = create_airports_df(fetched_df)
    assert isinstance(airports_df, pd.DataFrame)
    assert "airport_id" in airports_df.columns
    assert airports_df.shape[0] == 2


def test_create_dates_df(fetched_df):
    dates_df = create_dates_df(fetched_df)
    assert isinstance(dates_df, pd.DataFrame)
    assert "date_id" in dates_df.columns
    assert dates_df.shape[0] == 1
