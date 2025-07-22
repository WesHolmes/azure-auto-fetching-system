import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import time
from typing import Any, Dict, List, Set, Tuple

import aiohttp
from dotenv import load_dotenv
from tenacity import (
    TryAgain,
    retry,
    stop_after_attempt,
    wait_exponential,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class BackupApiError(Exception):
    pass


class BackupRadarAPI:
    """Client for interacting with the Backup Radar API."""

    ISO_DATE_FORMAT = "%Y-%m-%d"

    def __init__(self):
        self.api_key = os.getenv('BACKUP_RADAR_API_KEY')
        self.base_url = os.getenv('BACKUP_RADAR_BASE_URI').rstrip("/")
        self.rate_limit_seconds = 30
        self.timeout = 60
        self.max_retries = 3
        self._last_request_time = 0
        self._retired_policies_cache = None

    @staticmethod
    def get_utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def format_date_for_api(dt: datetime) -> str:
        return dt.strftime(BackupRadarAPI.ISO_DATE_FORMAT)

    async def _handle_rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Dict[str, Any] = None,
    ) -> Any:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {"ApiKey": self.api_key, "Content-Type": "application/json"}

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    ssl=False,
                ) as response:
                    response.raise_for_status()
                    return await response.json()

        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                logger.error("Authentication failed. Check your credentials.")
            elif e.status == 403:
                logger.error("Access denied. Check your permissions.")
            elif e.status == 404:
                logger.error(f"Resource not found: {url}")
            elif e.status == 429:
                retry_after = int(e.headers.get("Retry-After", 60))
                logger.warning(f"Rate limit exceeded (HTTP 429). Retrying after tenacity wait (Retry-After: {retry_after}s)...")
                raise TryAgain
            else:
                logger.error(f"HTTP error: {e.status} - {e.message}")
            raise BackupApiError(f"HTTP {e.status}: {e.message}")

        except asyncio.TimeoutError:
            logger.error(f"Request timed out after {self.timeout} seconds")
            raise BackupApiError("Request timed out")

        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            raise BackupApiError(f"Request failed: {str(e)}")

    async def _fetch_policies_for_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        params = {
            "date": self.format_date_for_api(start_date),
            "dateTo": self.format_date_for_api(end_date),
            "page": 1,
            "size": 1000,
        }
        all_policies = []
        try:
            while True:
                await self._handle_rate_limit()

                result = await self._make_request("GET", "policies", params=params)

                if isinstance(result, list):
                    policies = result
                elif isinstance(result, dict):
                    policies = result.get("data", [])
                else:
                    raise ValueError(f"Unexpected API response format: {type(result)}")

                if not isinstance(policies, list):
                    raise ValueError(f"Unexpected policies format: {type(policies)}")

                all_policies.extend(policies)

                if len(policies) < params["size"]:
                    break
                params["page"] += 1

            return all_policies

        except Exception as e:
            logger.error(f"Error fetching policies: {str(e)}")
            raise

    async def fetch_policies(self, start_date: datetime, end_date: datetime, chunk_size_days: int = 7) -> List[Dict[str, Any]]:
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        if start_date > end_date:
            logger.warning(f"Start date ({self.format_date_for_api(start_date)}) is after end date ({self.format_date_for_api(end_date)}). No policies will be fetched.")
            return []

        if start_date == end_date:
            logger.info(f"Fetching policies specifically for single date: {self.format_date_for_api(start_date)}")
            single_day_policies = await self._fetch_policies_for_date_range(start_date, end_date)
            logger.info(f"Total policies fetched for single day: {len(single_day_policies)}")
            return single_day_policies

        all_policies = []
        current_end = end_date
        current_start = max(current_end - timedelta(days=chunk_size_days - 1), start_date)

        while current_end >= start_date:
            current_start = max(current_start, start_date)

            chunk_policies = await self._fetch_policies_for_date_range(current_start, current_end)
            all_policies.extend(chunk_policies)

            current_end = current_start - timedelta(days=1)
            current_start = current_end - timedelta(days=chunk_size_days - 1)

            if current_end < start_date:
                break

        logger.info(f"Total policies fetched for range {self.format_date_for_api(start_date)} to {self.format_date_for_api(end_date)}: {len(all_policies)}")
        return all_policies

    async def _get_retired_policies_lookup(self) -> Set[Tuple[str, str]]:
        if self._retired_policies_cache is None:
            try:
                result = await self._make_request("GET", "policies/retired")
                if isinstance(result, list):
                    retired = result
                elif isinstance(result, dict):
                    retired = result.get("data", [])
                else:
                    raise ValueError(f"Unexpected API response format: {type(result)}")

                if retired and len(retired) > 0:
                    export_dir = os.path.join(os.getcwd(), "exports")
                    os.makedirs(export_dir, exist_ok=True)

                    current_date = self.format_date_for_api(self.get_utc_now())
                    export_file = os.path.join(export_dir, f"retired_policies_{current_date}.json")

                    simplified_retired = [
                        {
                            "companyName": p.get("companyName", ""),
                            "deviceName": p.get("deviceName", ""),
                        }
                        for p in retired
                        if p.get("companyName") and p.get("deviceName")
                    ]

                    with open(export_file, "w") as f:
                        json.dump(simplified_retired, f, indent=2)
                    logger.info(f"Exported {len(simplified_retired)} retired policies to {export_file}")

                self._retired_policies_cache = {
                    (
                        str(p.get("companyName", "")).lower(),
                        str(p.get("deviceName", "")).lower(),
                    )
                    for p in retired
                    if p.get("companyName") and p.get("deviceName")
                }
            except Exception as e:
                logger.error(f"Error fetching retired policies: {str(e)}")
                self._retired_policies_cache = set()

        return self._retired_policies_cache

    async def fetch_active_policies(
            self,
            start_date: datetime,
            end_date: datetime,
            chunk_size_days: int = 7
        ) -> List[Dict[str, Any]]:
        all_policies = await self.fetch_policies(start_date, end_date, chunk_size_days)
        logger.info(f"Fetched {len(all_policies)} policies")
        retired_policies = await self.fetch_retired_policies()
        logger.info(f"Fetched {len(retired_policies)} retired policies")

        active_policies = [p for p in all_policies if p.get("policyId") not in retired_policies]
        logger.info(f"Found {len(active_policies)} active policies out of {len(all_policies)} total policies")
        return active_policies

    async def fetch_retired_policies(self) -> List[Dict[str, Any]]:
        try:
            logger.info("Making API request to fetch retired policies...")
            result = await self._make_request("GET", "policies/retired")
            if isinstance(result, list):
                response = result
            elif isinstance(result, dict):
                response = result.get("data", [])
            else:
                raise ValueError(f"Unexpected API response format: {type(result)}")

            return response
        except Exception as e:
            logger.error(f"Error fetching retired policies: {str(e)}", exc_info=True)
            return []
