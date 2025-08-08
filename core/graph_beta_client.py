import time
import requests
import msal
import os
import logging

# Note: time.sleep() is acceptable here because:
# 1. Azure Functions handles scaling automatically
# 2. These are legitimate API rate limits that must be respected
# 3. The GraphBetaClient is synchronous by design


class GraphBetaClient:
    def __init__(self, tenant_id):
        if not tenant_id:
            raise ValueError("TenantID is needed")

        self.tenant_id = tenant_id
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.base_url = "https://graph.microsoft.com/beta"
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

            # If top parameter was specified, respect it and don't follow pagination
            if top and len(all_results) >= top:
                return all_results[:top]

            url = data.get("@odata.nextLink")

        return all_results

    def create_user(self, user_data):
        """Create a new user account via Microsoft Graph Beta API"""
        
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }
            
            # Microsoft Graph Beta API endpoint to create user
            url = f"{self.base_url}/users"
            
            # Debug logging
            logging.info(f"Creating user in tenant: {self.tenant_id}")
            logging.info(f"Graph Beta API URL: {url}")
            logging.info(f"User data: {user_data}")
            
            response = requests.post(url, headers=headers, json=user_data)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while creating user - waiting {retry_after} seconds")
                time.sleep(retry_after)
                # retry the request
                response = requests.post(url, headers=headers, json=user_data)
            
            # Enhanced error handling
            if response.status_code == 401:
                error_msg = f"401 Unauthorized - Cannot create user: Authentication failed"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 403:
                error_msg = f"403 Forbidden - Cannot create user: Insufficient permissions"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 409:
                error_msg = f"409 Conflict - User already exists or duplicate userPrincipalName"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 400:
                try:
                    error_details = response.json()
                    error_msg = f"400 Bad Request - Invalid user data: {error_details.get('error', {}).get('message', 'Unknown error')}"
                except:
                    error_msg = f"400 Bad Request - Invalid user data"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 503:
                error_msg = f"503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}
            
            # Check for success (201 Created is expected for POST operations)
            if response.status_code == 201:
                created_user = response.json()
                logging.info(f"Successfully created user {created_user.get('userPrincipalName', 'Unknown')}")
                return {
                    "status": "success", 
                    "message": f"User {created_user.get('userPrincipalName', 'Unknown')} created successfully",
                    "data": created_user
                }
            
            # Handle other error status codes
            logging.error(f"Graph Beta API returned status code: {response.status_code}")
            logging.error(f"Response headers: {dict(response.headers)}")
            try:
                error_details = response.json()
                logging.error(f"Response body: {error_details}")
            except:
                logging.error(f"Response text: {response.text}")
            
            response.raise_for_status()
            return {"status": "success", "message": "User created successfully"}
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error while creating user: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}
            
        except Exception as e:
            error_msg = f"Unexpected error while creating user: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}

    def delete_user(self, user_id):
        """Delete a user account via Microsoft Graph Beta API"""
        
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }
            
            # Microsoft Graph Beta API endpoint to delete user
            url = f"{self.base_url}/users/{user_id}"
            
            # Debug logging
            logging.info(f"Deleting user {user_id} in tenant: {self.tenant_id}")
            logging.info(f"Graph Beta API URL: {url}")
            
            response = requests.delete(url, headers=headers)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while deleting user - waiting {retry_after} seconds")
                time.sleep(retry_after)
                # retry the request
                response = requests.delete(url, headers=headers)
            
            # Enhanced error handling
            if response.status_code == 401:
                error_msg = f"401 Unauthorized - Cannot delete user: Authentication failed"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 403:
                error_msg = f"403 Forbidden - Cannot delete user: Insufficient permissions"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 404:
                error_msg = f"404 Not Found - User {user_id} not found"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 400:
                try:
                    error_details = response.json()
                    error_msg = f"400 Bad Request - Invalid request: {error_details.get('error', {}).get('message', 'Unknown error')}"
                except:
                    error_msg = f"400 Bad Request - Invalid request"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}
                
            elif response.status_code == 503:
                error_msg = f"503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}
            
            # Check for success (204 No Content is expected for DELETE operations)
            if response.status_code == 204:
                logging.info(f"Successfully deleted user {user_id}")
                return {
                    "status": "success", 
                    "message": f"User {user_id} deleted successfully"
                }
            
            # Handle other error status codes
            logging.error(f"Graph Beta API returned status code: {response.status_code}")
            logging.error(f"Response headers: {dict(response.headers)}")
            try:
                error_details = response.json()
                logging.error(f"Response body: {error_details}")
            except:
                logging.error(f"Response text: {response.text}")
            
            response.raise_for_status()
            return {"status": "success", "message": "User deleted successfully"}
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error while deleting user: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}
            
        except Exception as e:
            error_msg = f"Unexpected error while deleting user: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}
