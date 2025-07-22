import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CwAutomateAPI:
    def __init__(self):
        self.base_uri = os.getenv('CW_AUTOMATE_BASE_URI', '')
        self.client_id = os.getenv('CW_AUTOMATE_CLIENT_ID', '')
        self.username = os.getenv('CW_AUTOMATE_USERNAME', '')
        self.password = os.getenv('CW_AUTOMATE_PASSWORD', '')
        self.api_token: Optional[str] = None
        self.timeout = aiohttp.ClientTimeout(total=60)
        self.max_retries = 3
        self.page_size = 100
        self._session = None

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)

    async def _authenticate(self) -> None:
        if self.api_token:
            return

        url = f"{self.base_uri}apitoken/"
        payload = {"UserName": self.username, "Password": self.password}
        headers = {
            "Content-Type": "application/json",
            "ClientID": self.client_id,
        }

        try:
            await self._ensure_session()
            async with self._session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    self.api_token = data["AccessToken"]
                else:
                    error_text = await response.text()
                    raise Exception(f"Authentication failed: {error_text}")
        except Exception as e:
            logger.error(f"Failed to authenticate: {str(e)}")
            raise

    async def _make_request(self, method: str, url: str, **kwargs) -> Any:
        await self._ensure_session()

        if not self.api_token:
            await self._authenticate()

        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "ClientID": self.client_id,
            "Content-Type": "application/json",
        }
        headers.update(kwargs.pop("headers", {}))
        kwargs["headers"] = headers

        last_error = None
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    await asyncio.sleep(2 ** (attempt - 1))

                async with self._session.request(method, url, **kwargs) as response:
                    if response.status in (200, 201, 204):
                        if method.lower() == "head":
                            return True
                        return await response.json()

                    error_text = await response.text()

                    if response.status == 401:
                        self.api_token = None
                        await self._authenticate()
                        kwargs["headers"]["Authorization"] = f"Bearer {self.api_token}"
                        continue

                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", "5"))
                        await asyncio.sleep(retry_after)
                        continue

                    if 500 <= response.status < 600:
                        continue

                    raise Exception(f"API error ({response.status}): {error_text}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    continue
            except Exception as e:
                last_error = e
                break

        raise Exception(f"Failed after {self.max_retries} attempts. Last error: {str(last_error)}")

    async def get_clients(self) -> List[Dict[str, Any]]:
        clients = []
        page = 1
        while True:
            try:
                params = {
                    "page": page,
                    "pageSize": self.page_size,
                    "includeFields": "Id,Name,ExternalId"
                    }
                url = f"{self.base_uri}clients/"
                response = await self._make_request("GET", url, params=params)

                if not response:
                    break

                clients.extend(response)
                page += 1
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error fetching clients page {page}: {str(e)}")
                break

        return [{
            "automate_company_id": client.get("Id"),
            "connectwise_company_id": client.get("ExternalId"),
            "company": client.get("Name")}
            for client in clients
            ]

    async def get_computers(self, days_threshold: int = 45) -> List[Dict[str, Any]]:
        """Fetch active computers, filtering by days since last contact."""
        computers = []
        page = 1

        fields = [
            "Id",
            "Client",
            "ComputerName",
            "OperatingSystemName",
            "OperatingSystemVersion",
            "DomainName",
            "AssetDate",
            "AssetTag",
            "SerialNumber",
            "LocalIPAddress",
            "GatewayIPAddress",
            "Type",
            "RemoteAgentLastContact",
            "MacAddress",
        ]

        while True:
            try:
                params = {
                    "includeFields": ",".join(fields),
                    "conditions": 'Type="workstation" OR Type="server"',
                    "orderby": "Id asc",
                    "page": page,
                    "pageSize": self.page_size,
                }

                url = f"{self.base_uri}computers/"
                response = await self._make_request("GET", url, params=params)

                if not response:
                    break

                computers.extend(response)
                page += 1
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error fetching computers page {page}: {str(e)}")
                break

        cutoff_date = datetime.now() - timedelta(days=days_threshold)
        active_computers = []

        for computer in computers:
            # Handle API response capitalization inconsistencies
            last_contact = computer.get("RemoteAgentLastContact")
            if not last_contact and "remoteAgentLastContact" in computer:
                last_contact = computer["remoteAgentLastContact"]

            if not last_contact:
                continue

            try:
                contact_date = datetime.strptime(last_contact, "%Y-%m-%dT%H:%M:%S")
                if contact_date > cutoff_date:
                    active_computers.append(computer)
            except (ValueError, TypeError):
                continue

        return active_computers

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
