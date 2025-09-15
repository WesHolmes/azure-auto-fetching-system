from datetime import UTC, datetime, timedelta
import os

from dotenv import load_dotenv
import requests


load_dotenv()


class APIError(Exception):
    pass


def _headers():
    key = os.getenv("BACKUP_RADAR_API_KEY")
    if not key:
        raise APIError("Missing BACKUP_RADAR_API_KEY")
    return {"ApiKey": key, "Content-Type": "application/json"}


def get_backups(days_back: int = 7) -> dict:
    start = datetime.now(UTC) - timedelta(days=days_back)
    params = {"page": 1, "size": 1000, "date": start.strftime("%Y-%m-%d")}

    resp = requests.get(f"{os.getenv('BACKUP_RADAR_BASE_URI')}/backups", headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_backup_retired() -> dict:
    resp = requests.get(f"{os.getenv('BACKUP_RADAR_BASE_URI')}/backups/retired", headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_backup_overview() -> dict:
    resp = requests.get(f"{os.getenv('BACKUP_RADAR_BASE_URI')}/backups/overview", headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_backup_filters() -> dict:
    resp = requests.get(f"{os.getenv('BACKUP_RADAR_BASE_URI')}/backups/filters", headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    print(get_backups(1))
