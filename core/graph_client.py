import time
import requests
import msal
import os
import logging

# Note: time.sleep() is acceptable here because:
# 1. Azure Functions handles scaling automatically
# 2. These are legitimate API rate limits that must be respected
# 3. The GraphClient is synchronous by design


class GraphClient:
    def __init__(self, tenant_id):
        if not tenant_id:
            raise ValueError("TenantID is needed")

        self.tenant_id = tenant_id
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.token = None
        self.token_expires = 0

    def get_token(self):
        if self.token and time.time() < self.token_expires:
            return self.token

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )

        result = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )

        if "access_token" in result:
            self.token = result["access_token"]
            self.token_expires = time.time() + result.get("expires_in", 3600) - 300
            return self.token
        else:
            raise Exception(
                f"Token acquisition failed: {result.get('error', 'Unknown error')}"
            )

    def get(
        self,
        endpoint,
        select=None,
        expand=None,
        filter=None,
        count=False,
        top=None,
        order_by=None,
    ):
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
            "Content-Type": "application/json",
        }
        if count:
            headers["ConsistencyLevel"] = "eventual"

        url = f"{self.base_url}{endpoint}"
        all_results = []

        while url:
            # Only use params for the first request, pagination URLs already include parameters
            current_params = params if not all_results else None
            response = requests.get(url, headers=headers, params=current_params)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited - waiting {retry_after} seconds")
                time.sleep(retry_after)
                continue

            # Enhanced error handling with detailed diagnostics
            if response.status_code == 401:
                error_msg = f"401 Unauthorized - Tenant {self.tenant_id}: Authentication failed. "
                try:
                    error_details = response.json()
                    if "error" in error_details:
                        error_msg += f"Error: {error_details['error'].get('code', 'Unknown')} - {error_details['error'].get('message', 'No details')}"
                except Exception:
                    error_msg += "Likely causes: Missing admin consent, expired credentials, or tenant suspended."
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg, response=response)

            elif response.status_code == 403:
                error_msg = f"403 Forbidden - Tenant {self.tenant_id}: Insufficient permissions. "
                try:
                    error_details = response.json()
                    if "error" in error_details:
                        error_msg += f"Error: {error_details['error'].get('code', 'Unknown')} - {error_details['error'].get('message', 'No details')}"
                except Exception:
                    error_msg += "Likely causes: Missing Graph permissions, conditional access policies, or security defaults."
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg, response=response)

            elif response.status_code == 503:
                error_msg = f"503 Service Unavailable - Tenant {self.tenant_id}: Microsoft Graph service temporarily unavailable."
                logging.warning(error_msg + " Retrying after delay...")
                # For service unavailable, implement exponential backoff
                retry_count = getattr(self, "_retry_count", 0)
                self._retry_count = retry_count + 1
                wait_time = min(30, 5 * (2**retry_count))  # Max 30 seconds
                logging.info(
                    f"Waiting {wait_time} seconds before retry #{self._retry_count}"
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            data = response.json()

            results = data.get("value", [])
            all_results.extend(results)

            # if top parameter was specified, respect it and don't follow pagination
            if top and len(all_results) >= top:
                return all_results[:top]

            url = data.get("@odata.nextLink")

        return all_results

    def disable_user(self, user_id):
        """Disable a user account by setting accountEnabled to False"""
        
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }
            
            # microsoft graph API endpoint to update user
            url = f"{self.base_url}/users/{user_id}"
            
            # request body to disable the user
            data = {
                "accountEnabled": False
            }
            
            response = requests.patch(url, headers=headers, json=data)
            
            # handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while disabling user - waiting {retry_after} seconds")
                time.sleep(retry_after)
                # retry the request
                response = requests.patch(url, headers=headers, json=data)
            
            # enhanced error handling
            if response.status_code == 401:
                error_msg = f"401 Unauthorized - Cannot disable user {user_id}: Authentication failed"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 403:
                error_msg = f"403 Forbidden - Cannot disable user {user_id}: Insufficient permissions"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 404:
                error_msg = f"404 Not Found - User {user_id} does not exist"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 503:
                error_msg = f"503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}
            
            # check for success (204 No Content is expected for PATCH operations)
            if response.status_code in [200, 204]:
                logging.info(f"Successfully disabled user {user_id}")
                return {"status": "success", "message": f"User {user_id} disabled successfully"}
            
            # handle other error status codes
            response.raise_for_status()
            return {"status": "success", "message": f"User {user_id} disabled successfully"}
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error while disabling user {user_id}: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}
            
        except Exception as e:
            error_msg = f"Unexpected error while disabling user {user_id}: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}
