import requests
import json
from typing import Literal
import os
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")

headers = {"x-apikey": API_KEY}


def get_operator(operator_id: str) -> dict:
    endpoint = f"/operators/{operator_id}"
    response = requests.get(API_URL + endpoint, headers=headers)

    if response.status_code != 200:
        raise ValueError("error", response.status_code)

    operator = response.json()
    return operator
