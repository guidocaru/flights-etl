import os
from dotenv import dotenv_values


DIR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(DIR_PATH, ".env")

ENV = dotenv_values(dotenv_path)
