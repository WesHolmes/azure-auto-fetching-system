import asyncio
import base64
from datetime import datetime, timezone
import logging
import os
import re
from typing import Any, Dict, List
import aiohttp
from models import Contact, User


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectWiseError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class CwManageAPI:
    def __init__(self):
        """Initialize the ConnectWise service with configuration."""
        self.base_url = os.getenv('CW_MANAGE_BASE_URI')
        self.company_id = os.getenv('CW_MANAGE_COMPANY_ID', '')
        self.public_key = os.getenv('CW_MANAGE_PUBLIC_KEY', '')
        self.private_key = os.getenv('CW_MANAGE_PRIVATE_KEY', '')
        self.client_id = os.getenv('CW_MANAGE_CLIENT_ID', '')
        self.page_size = 1000
        self._session = None

    async def _ensure_session(self):
        """Ensure an active session exists, creating one if needed."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=60, connect=20)
            self._session = aiohttp.ClientSession(timeout=timeout)
            logger.info("Created new ConnectWise Manage API session")

    async def _get_auth_token(self) -> str:
        auth_str = f"{self.company_id}+{self.public_key}:{self.private_key}"
        return base64.b64encode(auth_str.encode()).decode("utf-8")

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Dict[str, Any] = None,
        json: Any = None,
        timeout: int = 30,
        return_headers: bool = False
    ) -> Dict[str, Any]:
        await self._ensure_session()

        # Build URL
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        # Add headers
        token = await self._get_auth_token()
        headers = {
            "Authorization": f"Basic {token}",
            "ClientId": self.client_id,
            "Content-Type": "application/json",
            "Accept": "application/vnd.connectwise.com+json; version=2025.1",
        }

        # Build request kwargs
        request_kwargs = {
            "headers": headers,
            "timeout": timeout,
        }
        if params:
            request_kwargs["params"] = params
        if json is not None:
            request_kwargs["json"] = json

        # Attempt request with retries
        max_retries = 3
        retry_delay = 1
        last_error = None

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt + 1} for {method} request to {endpoint}")

                async with self._session.request(method, url, **request_kwargs) as response:
                    if response.status == 429:  # Rate limit
                        await asyncio.sleep(retry_delay * (2**attempt))
                        continue

                    response.raise_for_status()

                    # Return data, possibly with headers
                    data = await response.json()
                    if return_headers:
                        return {
                            "data": data,
                            "headers": dict(response.headers),
                        }
                    return data

            except Exception as e:
                last_error = e
                logger.error(f"Request error (attempt {attempt + 1}): {str(e)}")

                # Retry if not the last attempt
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2**attempt))
                    continue

        # If we get here, all attempts failed
        error_msg = f"API request failed after {max_retries} attempts: {str(last_error)}"
        logger.error(error_msg)
        raise ConnectWiseError(error_msg)

    async def close(self):
        """Close the API connection and release resources."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def get_active_companies(self) -> List[Dict[str, Any]]:
        """Fetch all active companies."""
        endpoint = "company/companies"
        params = {
            "conditions": "status/name contains 'Active'",
            "page": 1,
            "pageSize": 1000,
            "fields": "id,identifier,name,status,customFields",
        }

        try:
            return await self._make_request("GET", endpoint, params=params)
        except Exception as e:
            logger.error(f"Failed to fetch active companies: {str(e)}")
            return []

    async def get_company_contacts(self, company_id: str) -> List[Contact]:
        """Fetch all contacts for a specific company."""
        contacts = []
        page = 1
        endpoint = "company/contacts"

        while True:
            params = {
                "fields": "id,firstName,lastName,company,communicationItems,inactiveFlag,securityIdentifier,userDefinedField1",
                "conditions": f"company/id={company_id} AND inactiveFlag=False",
                "pageSize": self.page_size,
                "page": page,
            }

            try:
                result = await self._make_request("GET", endpoint, params=params)
                if not result:
                    break

                contacts.extend(self._convert_to_contacts(result))

                if len(result) < self.page_size:
                    break

                page += 1
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Error fetching company contacts: {str(e)}")
                break

        return contacts

    async def get_all_contacts(self) -> List[Contact]:
        """Fetch all contacts with pagination support."""
        contacts = []

        try:
            # Get first page with pagination info
            initial_response = await self._get_contacts_page(1, initial_run=True)
            contacts.extend(self._convert_to_contacts(initial_response["data"]))

            # Process remaining pages if any
            total_pages = initial_response["page_numbers"]
            if total_pages > 1:
                for page in range(2, total_pages + 1):
                    try:
                        result = await self._get_contacts_page(page)
                        contacts.extend(self._convert_to_contacts(result["data"]))
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Error on page {page}: {str(e)}")
                        await asyncio.sleep(2)

            return contacts

        except Exception as e:
            logger.error(f"Failed to fetch all contacts: {str(e)}")
            return []

    async def _get_contacts_page(self, page_number: int, initial_run: bool = False) -> Dict[str, Any]:
        """Get a page of contacts, with pagination info on initial run."""
        endpoint = "company/contacts"
        params = {
            "fields": "id,firstName,lastName,company,communicationItems,inactiveFlag,securityIdentifier,userDefinedField1",
            "conditions": "inactiveFlag=False",
            "pageSize": self.page_size,
            "page": page_number,
        }

        if initial_run:
            result = await self._make_request("GET", endpoint, params=params, return_headers=True)

            # Parse pagination info from headers
            total_pages = 1
            if "Link" in result["headers"]:
                last_page_match = re.search(r'page=(\d+)>; rel="last"', result["headers"]["Link"])
                if last_page_match:
                    total_pages = int(last_page_match.group(1))

            return {"data": result["data"], "page_numbers": total_pages}
        else:
            data = await self._make_request("GET", endpoint, params=params)
            return {"data": data, "page_numbers": None}

    def _convert_to_contacts(self, contact_data_list: List[Dict]) -> List[Contact]:
        """Convert raw API data to Contact objects."""
        contacts = []

        for contact_data in contact_data_list:
            # Find email in communication items
            email = None
            for item in contact_data.get("communicationItems", []):
                if item.get("value") and "@" in item.get("value", "") and item.get("communicationType") == "Email":
                    email = item.get("value").lower()
                    break

            if email:
                try:
                    contact = Contact(
                        id=str(contact_data["id"]),
                        email=email,
                        inactive=contact_data.get("inactiveFlag", False),
                        first_name=contact_data.get("firstName"),
                        last_name=contact_data.get("lastName"),
                        company_id=(str(contact_data.get("company", {}).get("id")) if contact_data.get("company", {}).get("id") else None),
                        company_name=contact_data.get("company", {}).get("name"),
                        security_identifier=contact_data.get("securityIdentifier"),
                        user_defined_field1=contact_data.get("userDefinedField1"),
                    )
                    contacts.append(contact)
                except Exception as e:
                    logger.error(f"Error processing contact: {str(e)}")

        return contacts

    async def update_contact_with_ms_info(self, contact_id: str, ms_user: User) -> bool:
        """Update a contact with Microsoft Graph user information."""
        endpoint = f"company/contacts/{contact_id}"

        # Build update payload
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        last_sign_in = ms_user.lastSignInDate.strftime("%Y-%m-%d %H:%M:%S") if ms_user.lastSignInDate else "N/A"

        payload = {
            "userDefinedField1": ms_user.userPrincipalName[:49],
            "userDefinedField2": last_sign_in,
            "userDefinedField3": timestamp,
            "userDefinedField8": str(ms_user.accountEnabled),
        }

        if ms_user.tenant_id:
            payload["userDefinedField4"] = ms_user.tenant_id
        if ms_user.tenant_name:
            payload["userDefinedField5"] = ms_user.tenant_name[:49]

        try:
            await self._make_request("PATCH", endpoint, json=payload)
            return True
        except Exception as e:
            logger.error(f"Failed to update contact {contact_id} with MS info: {str(e)}")
            return False

    async def update_contact_to_inactive_status(self, contact_id: str, last_name: str = "") -> bool:
        """Mark a contact as inactive in ConnectWise."""
        endpoint = f"company/contacts/{contact_id}"

        payload = [
            {"op": "replace", "path": "lastName", "value": f"{last_name} [inactive]".strip() if last_name else "[inactive]"},
            {"op": "replace", "path": "inactiveFlag", "value": True},
            {"op": "replace", "path": "userDefinedField8", "value": "false"},
        ]

        try:
            result = await self._make_request("PATCH", endpoint, json=payload)
            return result.get("inactiveFlag", False)
        except Exception as e:
            logger.error(f"Failed to mark contact {contact_id} as inactive: {str(e)}")
            return False

    async def get_tag_contacts(self) -> List[Dict[str, Any]]:
        """Get contacts with specific tags (approver, VIP, POC, internal IT)."""
        endpoint = "company/contacts"
        params = {
            "fields": "id,firstName,lastName,company,communicationItems,types",
            "childConditions": (f"types/id={os.getenv('CW_APPROVER_TYPE_ID', '0')} OR "
                              f"types/id={os.getenv('CW_VIP_TYPE_ID', '0')} OR "
                              f"types/id={os.getenv('CW_POC_TYPE_ID', '0')} OR "
                              f"types/id={os.getenv('CW_INTERNAL_IT_TYPE_ID', '0')}"),
            "conditions": "inactiveFlag=False",
            "pageSize": self.page_size,
        }

        try:
            return await self._make_request("GET", endpoint, params=params)
        except Exception as e:
            logger.error(f"Failed to fetch tag contacts: {str(e)}")
            return []

    async def search_contact_by_name(self, name: str, company_name: str = None) -> List[Dict[str, Any]]:
        """Search contacts by name, optionally filtered by company."""
        name_parts = name.strip().split()

        # Build name conditions
        if len(name_parts) >= 2:
            name_cond = f'(firstName like "{name_parts[0]}%" OR lastName like "{name_parts[-1]}%")'
        else:
            name_cond = f'(firstName like "{name_parts[0]}%" OR lastName like "{name_parts[0]}%")'

        # Add company filter if provided
        conditions = name_cond
        if company_name:
            conditions += f' AND company/name like "{company_name}%"'

        endpoint = "company/contacts"
        params = {
            "fields": "id,firstName,lastName,company,communicationItems,types,inactiveFlag",
            "conditions": conditions,
            "pageSize": 50,
        }

        try:
            return await self._make_request("GET", endpoint, params=params)
        except Exception as e:
            logger.error(f"Contact search failed: {str(e)}")
            return []

    async def get_contact_by_id(self, contact_id: str) -> Dict[str, Any]:
        """Get a specific contact by ID."""
        endpoint = f"company/contacts/{contact_id}"
        params = {
            "fields": "id,firstName,lastName,company,communicationItems,inactiveFlag,securityIdentifier,userDefinedField1"
        }

        try:
            return await self._make_request("GET", endpoint, params=params)
        except Exception as e:
            logger.error(f"Failed to fetch contact {contact_id}: {str(e)}")
            return {}

    async def get_active_devices(self) -> List[Dict[str, Any]]:
        """Fetch all active workstation and server devices."""
        devices = []
        page = 0
        endpoint = "company/configurations"

        try:
            while True:
                params = {
                    "page": page,
                    "pageSize": 500,
                    "orderBy": "name",
                    "fields": "company/name,company/id,company/identifier,name,id,status/name,type/name,serialNumber,modelNumber,macAddress,osType",
                    "conditions": "(status/id not in (2,6) and type/id in (18,19))",
                }

                result = await self._make_request("GET", endpoint, params=params)
                if not result:
                    break

                devices.extend(result)

                if len(result) < 500:
                    break

                page += 1
                await asyncio.sleep(0.5)

            return devices

        except Exception as e:
            logger.error(f"Failed to fetch devices: {str(e)}")
            raise

    def _flatten_device_data(self, device_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested device data into a simple dictionary."""
        return {
            "company_name": device_data.get("company", {}).get("name", ""),
            "company_id": device_data.get("company", {}).get("id"),
            "company_identifier": device_data.get("company", {}).get("identifier"),
            "name": device_data.get("name", ""),
            "id": device_data.get("id"),
            "type": device_data.get("type", {}).get("name", ""),
            "status": device_data.get("status", {}).get("name", ""),
            "serial_number": device_data.get("serialNumber", ""),
            "model_number": device_data.get("modelNumber", ""),
            "mac_address": device_data.get("macAddress", ""),
            "os": device_data.get("osType", ""),
        }
