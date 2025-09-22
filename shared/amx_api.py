from collections.abc import Iterator
from datetime import datetime
import logging
import os
import time
from typing import Any

import backoff
from dotenv import load_dotenv
import pytz
import requests
from requests.exceptions import RequestException

from .config import load_local_settings


# Load configuration
load_dotenv()  # Load .env file first
load_local_settings()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def format_datetime(dt_str: str | None) -> str | None:
    """Format datetime string to ISO format with proper timezone handling."""
    if not dt_str:
        return None

    try:
        if isinstance(dt_str, str):
            # Handle various datetime formats from Automox API
            if "T" in dt_str:
                # ISO format - handle Z suffix and timezone info
                if dt_str.endswith("Z"):
                    dt_str = dt_str.replace("Z", "+00:00")

                # Parse the datetime
                dt = datetime.fromisoformat(dt_str)

                # If it's naive (no timezone info), assume UTC
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)

                # Convert to UTC and return ISO format
                return dt.astimezone(pytz.UTC).isoformat()
            else:
                # Try parsing as timestamp or other format
                try:
                    # Try as Unix timestamp
                    timestamp = float(dt_str)
                    dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
                    return dt.isoformat()
                except (ValueError, TypeError):
                    # Fallback to string representation
                    return str(dt_str)

        return str(dt_str) if dt_str else None

    except (ValueError, TypeError, OSError) as e:
        logger.warning(f"Failed to parse datetime '{dt_str}': {e}")
        return str(dt_str) if dt_str else None


class AutomoxError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class AutomoxApi:
    def __init__(self):
        self.base_uri = os.environ["AMX_BASE_URI"]
        self.dit_api_key = os.environ["AMX_DIT_API_KEY"]
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def get_api_key(self) -> str:
        return self.dit_api_key

    def _paginate_request(
        self,
        endpoint: str,
        params: dict[str, Any],
        page_key: str = "page",
        limit_key: str = "limit",
        limit_value: int = 500,
        max_pages: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Generic pagination helper for Automox API endpoints."""
        page = 0
        total_pages = None

        while True:
            if max_pages and page >= max_pages:
                break

            current_params = params.copy()
            current_params[page_key] = page
            current_params[limit_key] = limit_value

            try:
                response = self.request("GET", endpoint, self.get_api_key(), params=current_params)

                if not response:
                    break

                # Handle different response formats
                if isinstance(response, dict):
                    if "data" in response:
                        data = response["data"]
                        total_pages = response.get("total_pages")
                    else:
                        data = response
                else:
                    data = response

                if not data:
                    break

                yield data
                page += 1

                # Break if we've reached total pages
                if total_pages is not None and page >= total_pages:
                    break

                # Rate limiting
                time.sleep(1)

            except AutomoxError as e:
                # Handle 403 errors gracefully (permission denied)
                if e.status_code == 403:
                    logger.warning(f"Access denied for pagination request: {str(e)}")
                    break
                elif page > 0:
                    logger.warning(f"Partial data retrieved before error: {str(e)}")
                    break
                else:
                    raise

    def _transform_device_data(self, device: dict[str, Any]) -> dict[str, Any]:
        """Transform raw device data into standardized format."""
        return {
            "id": device.get("id"),
            "org_id": device.get("organization_id"),
            "name": device.get("name"),
            "agent_version": device.get("agent_version"),
            "is_compliant": device.get("status", {}).get("policy_status") == "compliant",
            "is_connected": device.get("connected"),
            "create_time": format_datetime(device.get("create_time")),
            "mdm_server": device.get("detail", {}).get("MDM_SERVER"),
            "mdm_profile_installed": device.get("detail", {}).get("MDM_PROFILE_INSTALLED") == "true",
            "version": device.get("detail", {}).get("VERSION"),
            "secure_token_account": device.get("detail", {}).get("SECURE_TOKEN_ACCOUNT"),
            "model": device.get("detail", {}).get("MODEL"),
            "vendor": device.get("detail", {}).get("VENDOR"),
            "serial_number": device.get("serial_number"),
            "os_version": device.get("os_version"),
            "os_version_id": device.get("os_version_id"),  # Now available with include_details=1
            "server_group_id": device.get("server_group_id"),  # Now available with include_details=1
            "pending_patches": device.get("pending_patches"),
            "last_logged_in_user": device.get("last_logged_in_user"),
            "last_process_time": format_datetime(device.get("last_process_time")),
            "last_refresh_time": format_datetime(device.get("last_refresh_time")),
            "last_update_time": format_datetime(device.get("last_update_time")),
            "last_disconnect_time": format_datetime(device.get("last_disconnect_time")),
            "is_delayed_by_user": device.get("is_delayed_by_user"),
            "needs_reboot": device.get("needs_reboot"),
            "needs_attention": device.get("needs_attention"),
            "is_compatible": device.get("is_compatible"),
            "ip_addrs": ",".join(device.get("ip_addrs", [])),
            "ip_addrs_private": ",".join(device.get("ip_addrs_private", [])),  # Now available with include_details=1
            "os_family": device.get("os_family"),
            "os_name": device.get("os_name"),  # Now available with include_details=1
            "next_patch_time": format_datetime(device.get("next_patch_time")),  # Now available with include_next_patch_time=1
        }

    def _transform_package_data(self, package: dict[str, Any]) -> dict[str, Any]:
        """Transform raw package data into standardized format."""
        return {
            "id": package.get("id"),
            "organization_id": package.get("organization_id"),
            "server_id": package.get("server_id"),
            "package_id": package.get("package_id"),
            "software_id": package.get("software_id"),
            "installed": package.get("installed"),
            "ignored": package.get("ignored"),
            "group_ignored": package.get("group_ignored"),
            "name": package.get("name"),
            "display_name": package.get("display_name"),
            "version": package.get("version"),
            "repo": package.get("repo"),
            "cves": package.get("cves"),
            "cve_score": package.get("cve_score"),
            "agent_severity": package.get("agent_severity"),
            "severity": package.get("severity"),
            "package_version_id": package.get("package_version_id"),
            "os_name": package.get("os_name"),
            "os_version": package.get("os_version"),
            "os_version_id": package.get("os_version_id"),
            "create_time": package.get("create_time"),
            "requires_reboot": package.get("requires_reboot"),
            "patch_classification_category_id": package.get("patch_classification_category_id"),
            "patch_scope": package.get("patch_scope"),
            "is_uninstallable": package.get("is_uninstallable"),
            "secondary_id": package.get("secondary_id"),
            "is_managed": package.get("is_managed"),
            "impact": package.get("impact"),
            "deferred_until": package.get("deferred_until"),
            "group_deferred_until": package.get("group_deferred_until"),
            "is_deleted": 0,
        }

    @backoff.on_exception(
        backoff.expo,
        (RequestException, AutomoxError),
        max_tries=5,
        base=2,
        factor=1.5,
        giveup=lambda e: getattr(e, "status_code", None) in [401, 403, 404],
    )
    def request(self, method: str, endpoint: str, api_key: str, **kwargs) -> Any:
        url = f"{self.base_uri}{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        try:
            if method == "GET" and "params" in kwargs:
                params = kwargs["params"]
                kwargs["params"] = {k: str(v).strip() if isinstance(v, str | int) else v for k, v in params.items() if v is not None}

            response = self.session.request(method, url, headers=headers, timeout=30, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else None
        except RequestException as e:
            # Check if it's a 403 Forbidden error (permission issue)
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code == 403:
                # For 403 errors, raise a specific exception that can be handled gracefully
                raise AutomoxError(f"Access denied (403): {str(e)}", status_code)
            else:
                # For other errors, raise as before
                raise AutomoxError(str(e), status_code)

    def get_all_organizations(self) -> list[dict[str, Any]]:
        """Fetch all organizations."""
        try:
            orgs = self.request("GET", "orgs", self.dit_api_key, params={"limit": 500, "page": 0})
            return orgs if orgs else []
        except Exception as e:
            logger.error(f"Error fetching organizations: {e}")
            raise AutomoxError(f"Failed to fetch organizations: {str(e)}")

    def get_all_device_details_by_organization(self, org_id: int, limit: int = 500) -> list[dict[str, Any]]:
        """Fetch all device details for a specific organization."""
        devices_list = []

        # Include details to get server_group_id, os_version_id, and other detailed fields
        params = {"o": org_id, "include_details": 1, "include_server_events": 1, "include_next_patch_time": 1}

        for page_data in self._paginate_request("servers", params, limit_value=limit):
            devices_list.extend([self._transform_device_data(d) for d in page_data])

        return devices_list

    def get_packages_by_organization(self, org_id: int, org_name: str) -> list[dict[str, Any]]:
        """Fetch all packages for a specific organization."""
        packages_list = []

        for page_data in self._paginate_request(
            f"orgs/{org_id}/packages",
            {
                "id": org_id,
                "o": org_id,
                "include_unmanaged": 0,
            },
            limit_value=500,
        ):
            packages_list.extend([self._transform_package_data(package) for package in page_data])

        return packages_list

    def get_prepatch_report(self, org_id: int) -> dict[str, Any]:
        """Fetch prepatch report for a specific organization."""
        devices = []

        for page_data in self._paginate_request(
            "reports/prepatch", {"o": org_id}, page_key="offset", limit_key="limit", limit_value=250, max_pages=10
        ):
            # Extract devices from prepatch response format
            if isinstance(page_data, dict) and "prepatch" in page_data:
                page_devices = page_data["prepatch"].get("devices", [])
                devices.extend(page_devices)
            elif isinstance(page_data, list):
                devices.extend(page_data)

        return {"prepatch": {"devices": devices}}

    def get_all_policies_by_organization(self, org_id: int) -> list[dict[str, Any]]:
        """Fetch all policies for a specific organization."""
        try:
            policies = self.request(
                "GET",
                "policies",
                self.get_api_key(),
                params={"o": org_id, "limit": 500, "page": 0},
            )
            return policies if policies else []
        except Exception as e:
            logger.error(f"Error fetching policies for org {org_id}: {e}")
            raise AutomoxError(f"Failed to fetch policies: {str(e)}")
