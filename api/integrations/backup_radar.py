import asyncio
from datetime import datetime, timedelta
import os
import time
from typing import Any, Dict, List, Optional

import aiohttp





class BackupRadarAPI:
    def __init__(self):
        self.api_key = os.getenv('BACKUP_RADAR_API_KEY')
        self.base_url = os.getenv('BACKUP_RADAR_BASE_URI')
        self.rate_limit_seconds = 30
        self._last_request_time = 0
        self._retired_policies_cache = None
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _handle_rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.time()

    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{endpoint.lstrip('/')}"
        headers = {"ApiKey": self.api_key, "Content-Type": "application/json"}

        async def make_call():
            async with self.session.request(
                method, url, headers=headers, params=params, ssl=False
            ) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientError("Rate limited")
                response.raise_for_status()
                return await response.json()

        # Simple retry with exponential backoff
        for attempt in range(3):
            try:
                return await make_call()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)

    async def close(self):
        if self.session:
            await self.session.close()


    async def fetch_policies(self, start_date: datetime, end_date: datetime, chunk_size_days: int = 7) -> List[Dict[str, Any]]:
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        if start_date > end_date:
            return []

        all_policies = []
        current_start = start_date

        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=chunk_size_days - 1), end_date)

            # Inline the pagination logic
            params = {
                "date": current_start.strftime("%Y-%m-%d"),
                "dateTo": current_end.strftime("%Y-%m-%d"),
                "page": 1,
                "size": 1000,
            }

            while True:
                await self._handle_rate_limit()
                result = await self._make_request("GET", "policies", params)

                policies = result if isinstance(result, list) else result.get("data", [])
                all_policies.extend(policies)

                if len(policies) < params["size"]:
                    break
                params["page"] += 1

            current_start = current_end + timedelta(days=1)

        seen = set()
        unique_policies = []
        for policy in all_policies:
            policy_id = policy.get("policyId")
            if policy_id and policy_id not in seen:
                seen.add(policy_id)
                unique_policies.append(policy)

        return unique_policies

    async def fetch_retired_policies(self) -> List[Dict[str, Any]]:
        if self._retired_policies_cache is not None:
            return self._retired_policies_cache

        all_policies = []
        params = {"showRetiredPolicies": "true", "page": 1, "size": 1000}

        while True:
            await self._handle_rate_limit()
            result = await self._make_request("GET", "policies", params)

            policies = result if isinstance(result, list) else result.get("data", [])
            all_policies.extend(policies)

            if len(policies) < params["size"]:
                break
            params["page"] += 1

        self._retired_policies_cache = all_policies
        return all_policies

    def filter_policies_by_date(
        self, policies: List[Dict[str, Any]], start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        filtered = []
        for policy in policies:
            status_time_str = policy.get("statusTime")
            if not status_time_str:
                continue

            try:
                status_time = datetime.fromisoformat(status_time_str.replace("Z", "+00:00"))
                status_date = status_time.date()
                if start_date.date() <= status_date <= end_date.date():
                    filtered.append(policy)
            except (ValueError, AttributeError):
                continue

        return filtered

    async def get_all_policies_including_retired(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        active_policies, retired_policies = await asyncio.gather(
            self.fetch_policies(start_date, end_date),
            self.fetch_retired_policies()
        )

        filtered_retired = self.filter_policies_by_date(retired_policies, start_date, end_date)

        combined = active_policies + filtered_retired
        seen = set()
        unique_policies = []

        for policy in combined:
            policy_id = policy.get("policyId")
            if policy_id and policy_id not in seen:
                seen.add(policy_id)
                unique_policies.append(policy)

        return unique_policies