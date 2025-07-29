import os
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

OMIE_CLIENT = os.getenv("OMIE_CLIENT")
OMIE_SECRET = os.getenv("OMIE_SECRET")
OMIE_PERSONS_URL = os.getenv("OMIE_PERSONS_URL")
OMIE_FINANCIAL_URL = os.getenv("OMIE_FINANCIAL_URL")
OMIE_CITIES_URL = os.getenv("OMIE_CITIES_URL")


def get_config_path():
    return Path(__file__).parent / "config.json"


def load_config() -> dict:
    if not get_config_path().exists():
        return {}

    with open(get_config_path(), "r") as f:
        return json.load(f)


def get_config() -> dict:
    config = load_config()
    config.update(
        {
            "OMIE_CLIENT": OMIE_CLIENT,
            "OMIE_SECRET": OMIE_SECRET,
            "OMIE_PERSONS_URL": OMIE_PERSONS_URL,
            "OMIE_FINANCIAL_URL": OMIE_FINANCIAL_URL,
            "OMIE_CITIES_URL": OMIE_CITIES_URL,
        }
    )
    return config
