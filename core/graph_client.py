import time
import requests
import msal
import os
import asyncio
import aiohttp
from typing import List, Optional

class GraphClient:
    def __init__(self, tenant_id, version="v1.0"):
        if not tenant_id:
            raise ValueError("TenantID is needed")

        self.tenant_id = tenant_id
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.base_url = f"https://graph.microsoft.com/{version}"
        self.token = None
        self.token_expires = 0

        # Debug: Print out the loaded environment variables (for troubleshooting only)
        print(f"DEBUG: CLIENT_ID={self.client_id}")
        print(f"DEBUG: CLIENT_SECRET={'SET' if self.client_secret else 'NOT SET'}")
        print(f"DEBUG: TENANT_ID={self.tenant_id}")

    def get_token(self):
        print(f"DEBUG: Acquiring token for tenant {self.tenant_id} with client_id {self.client_id}")
        if self.token and time.time() < self.token_expires:
            print("DEBUG: Using cached token")
            return self.token

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret
        )

        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if "access_token" in result:
            print("DEBUG: Token acquired successfully")
            self.token = result["access_token"]
            self.token_expires = time.time() + result.get("expires_in", 3600) - 300
            return self.token
        else:
            print(f"ERROR: Token acquisition failed: {result}")
            raise Exception(f"Token acquisition failed: {result.get('error', 'Unknown error')}")

    def get(self, endpoint, select=None, expand=None, filter=None, count=False, top=999, order_by=None):
        print(f"DEBUG: GET {endpoint} for tenant {self.tenant_id}")
        params = {}
        if select:
            params["$select"] = ",".join(select)
        if expand:
            params["$expand"] = expand
        if filter:
            params["$filter"] = filter
        if top:
            params["$top"] = top
        if count:
            params["$count"] = "true"
        if order_by:
            params["$orderby"] = order_by

        headers = {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json"
        }
        if count:
            headers["ConsistencyLevel"] = "eventual"

        url = f"{self.base_url}{endpoint}"
        all_results = []

        while url:
            response = requests.get(url, headers=headers, params=params if not all_results else None)

            if response.status_code == 429:
                time.sleep(int(response.headers.get("Retry-After", 5)))
                continue

            response.raise_for_status()
            data = response.json()

            results = data.get("value", [])
            all_results.extend(results)

            url = data.get("@odata.nextLink")

        return all_results


class AsyncGraphClient:
    """Async version of GraphClient for improved performance"""
    
    def __init__(self, tenant_id, version="v1.0"):
        if not tenant_id:
            raise ValueError("TenantID is needed")

        self.tenant_id = tenant_id
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.base_url = f"https://graph.microsoft.com/{version}"
        self.token = None
        self.token_expires = 0
        self._session = None

    async def __aenter__(self):
        """Async context manager entry"""
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._session:
            await self._session.close()

    def get_token(self):
        """Synchronous token acquisition (MSAL doesn't have async support)"""
        print(f"DEBUG: Acquiring token for tenant {self.tenant_id} with client_id {self.client_id}")
        if self.token and time.time() < self.token_expires:
            print("DEBUG: Using cached token")
            return self.token

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret
        )

        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if "access_token" in result:
            print("DEBUG: Async token acquired successfully")
            self.token = result["access_token"]
            self.token_expires = time.time() + result.get("expires_in", 3600) - 300
            return self.token
        else:
            print(f"ERROR: Async token acquisition failed: {result}")
            raise Exception(f"Token acquisition failed: {result.get('error', 'Unknown error')}")

    async def get(self, endpoint, select=None, expand=None, filter=None, count=False, top=999, order_by=None):
        """Async GET request with automatic pagination"""
        print(f"DEBUG: Async GET {endpoint} for tenant {self.tenant_id}")
        
        params = {}
        if select:
            params["$select"] = ",".join(select)
        if expand:
            params["$expand"] = expand
        if filter:
            params["$filter"] = filter
        if top:
            params["$top"] = top
        if count:
            params["$count"] = "true"
        if order_by:
            params["$orderby"] = order_by

        headers = {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json"
        }
        if count:
            headers["ConsistencyLevel"] = "eventual"

        url = f"{self.base_url}{endpoint}"
        all_results = []

        while url:
            try:
                async with self._session.get(url, headers=headers, params=params if not all_results else None) as response:
                    
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        print(f"DEBUG: Rate limited, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    data = await response.json()

                    results = data.get("value", [])
                    all_results.extend(results)

                    url = data.get("@odata.nextLink")
                    
            except Exception as e:
                print(f"ERROR: Async API call failed for {endpoint}: {e}")
                raise

        return all_results

    async def get_multiple(self, requests_list: List[dict]) -> List:
        """Perform multiple GET requests concurrently"""
        print(f"DEBUG: Performing {len(requests_list)} concurrent requests")
        
        async def single_request(request_info):
            try:
                return await self.get(**request_info)
            except Exception as e:
                print(f"ERROR: Request failed: {e}")
                return []
        
        results = await asyncio.gather(*[single_request(req) for req in requests_list], return_exceptions=True)
        
        # Handle exceptions and return clean results
        clean_results = []
        for result in results:
            if isinstance(result, Exception):
                print(f"WARNING: Request failed: {result}")
                clean_results.append([])
            else:
                clean_results.append(result)
        
        return clean_results