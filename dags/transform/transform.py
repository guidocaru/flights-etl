import json
import pandas as pd
from utils.dates import yesterday


def transform_data(**context):
    data_pulled = context["ti"].xcom_pull(key="my_key")
    print("data desde transform", data_pulled)


def transform_data_mock(path: str):
    """
    Receives the fetched data and generates a Pandas dataframe for each table in the database.
    """

    with open(path, "r") as json_file:
        fetched = json.load(json_file)

    fetched_df = create_df(fetched)
    flights_df = create_flights_df(fetched_df)
    airports_df = create_airports_df(fetched_df)
    operators_df = create_operators_df(fetched_df)
    dates_df = create_dates_df(fetched_df)

    return {
        "flights_df": flights_df,
        "airports_df": airports_df,
        "operators_df": operators_df,
        "dates_df": dates_df,
    }


def create_df(data: list[dict]) -> pd.DataFrame:
    """
    Converts the list of fetched data dictionaries into a Pandas dataframe, selecting only the desired columns.

    Args:
        data (list[dict]): A list of dictionaries, where each one contains flight data fetched from the API.

    Returns:
        pd.DataFrame: a pandas Dataframe with the desired columns to later generate the different tables from it.
    """

    df = pd.DataFrame(
        [
            {
                "flight_id": flight["fa_flight_id"],
                "flight_number": flight["flight_number"],
                "date_id": yesterday(),
                "operator_id": flight["operator"],
                "origin_id": flight["origin"]["code_icao"],
                "destination_id": flight["destination"]["code_icao"],
                "departure_delay": flight["departure_delay"],
                "arrival_delay": flight["arrival_delay"],
                "origin_name": flight["origin"]["name"],
                "destination_name": flight["destination"]["name"],
                "origin_city": flight["origin"]["city"],
                "destination_city": flight["destination"]["city"],
                "scheduled_out": flight["scheduled_out"],
                "estimated_out": flight["estimated_out"],
                "actual_out": flight["actual_out"],
                "scheduled_in": flight["scheduled_in"],
                "estimated_in": flight["estimated_in"],
                "actual_in": flight["actual_in"],
            }
            for flight in data
        ]
    )

    df = df.dropna(subset=["operator_id"])

    datetime_columns = [
        "scheduled_out",
        "estimated_out",
        "actual_out",
        "scheduled_in",
        "estimated_in",
        "actual_in",
    ]
    df[datetime_columns] = df[datetime_columns].apply(pd.to_datetime)

    return df


def create_flights_df(fetched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a pandas dataframe representing the flights table.

    Args:
        fetched_df (pd.DataFrame): A pandas dataframe containing flight data fetched from the API.

    Returns:
        pd.DataFrame: A pandas dataframe containing selected flight information.
    """

    flights_columns = [
        "flight_id",
        "flight_number",
        "date_id",
        "operator_id",
        "origin_id",
        "destination_id",
        "scheduled_out",
        "estimated_out",
        "actual_out",
        "scheduled_in",
        "estimated_in",
        "actual_in",
        "departure_delay",
        "arrival_delay",
    ]
    flights_df = fetched_df[flights_columns]
    return flights_df


def create_airports_df(fetched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a dataframe containing unique airport information, representing the airports table.

    Args:
        fetched_df (pd.DataFrame): A pandas dataframe containing flight data fetched from the API.

    Returns:
        pd.DataFrame: A pandas dataframe with unique airport information.
    """

    origin_airports = fetched_df[["origin_id", "origin_name", "origin_city"]].rename(
        columns={
            "origin_id": "airport_id",
            "origin_name": "name",
            "origin_city": "city",
        }
    )

    destination_airports = fetched_df[
        ["destination_id", "destination_name", "destination_city"]
    ].rename(
        columns={
            "destination_id": "airport_id",
            "destination_name": "name",
            "destination_city": "city",
        }
    )

    unique_airports = pd.concat(
        [origin_airports, destination_airports], ignore_index=True
    ).drop_duplicates(subset="airport_id")

    return unique_airports


def create_operators_df(fetched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a dataframe of operators using the fetched flight data and a local JSON file with operator details.

    Args:
        fetched_df (pd.DataFrame): A pandas dataframe containing flight data fetched from the API.

    Returns:
        pd.DataFrame: A pandas dataframe with operators information.
    """

    operators_id = fetched_df["operator_id"].unique()

    with open("dags/transform/operators.json", "r") as json_file:
        operators_info = json.load(json_file)

    operators = [
        [
            operator_id,
            operators_info[operator_id]["name"],
            operators_info[operator_id]["country"],
        ]
        for operator_id in operators_id
    ]

    operators_df = pd.DataFrame(operators, columns=["operator_id", "name", "country"])

    return operators_df


def create_dates_df(fetched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a dataframe with date-related details from the fetched flight data.

    Args:
        fetched_df (pd.DataFrame): A pandas dataframe containing flight data fetched from the API.

    Returns:
        pd.DataFrame: A pandas dataframe containing date IDs and related information such as day, month, year, and quarter.
    """

    dates_id = fetched_df["date_id"].unique()

    dates = pd.to_datetime(dates_id, format="%Y-%m-%d")

    dates_df = pd.DataFrame(
        {
            "date_id": dates.to_pydatetime(),
            "day": dates.day,
            "month": dates.month,
            "year": dates.year,
            "day_of_week": dates.day_name(),
            "quarter": dates.quarter,
        }
    )

    return dates_df
