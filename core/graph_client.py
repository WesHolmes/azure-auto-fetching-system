import time
import requests
import msal
import os
import logging

class GraphClient:
    def __init__(self, tenant_id):
        if not tenant_id:
            raise ValueError("TenantID is needed")

        self.tenant_id = tenant_id
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.token = None
        self.token_expires = 0

    def get_token(self):
        if self.token and time.time() < self.token_expires:
            return self.token

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret
        )

        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if "access_token" in result:
            self.token = result["access_token"]
            self.token_expires = time.time() + result.get("expires_in", 3600) - 300
            return self.token
        else:
            raise Exception(f"Token acquisition failed: {result.get('error', 'Unknown error')}")

    def get(self, endpoint, select=None, expand=None, filter=None, count=False, top=None, order_by=None):
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
            # Only use params for the first request, pagination URLs already include parameters
            current_params = params if not all_results else None
            response = requests.get(url, headers=headers, params=current_params)

            if response.status_code == 429:
                time.sleep(int(response.headers.get("Retry-After", 5)))
                continue

            # Enhanced error handling with detailed diagnostics
            if response.status_code == 401:
                error_msg = f"401 Unauthorized - Tenant {self.tenant_id}: Authentication failed. "
                try:
                    error_details = response.json()
                    if 'error' in error_details:
                        error_msg += f"Error: {error_details['error'].get('code', 'Unknown')} - {error_details['error'].get('message', 'No details')}"
                except:
                    error_msg += "Likely causes: Missing admin consent, expired credentials, or tenant suspended."
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg, response=response)
            
            elif response.status_code == 403:
                error_msg = f"403 Forbidden - Tenant {self.tenant_id}: Insufficient permissions. "
                try:
                    error_details = response.json()
                    if 'error' in error_details:
                        error_msg += f"Error: {error_details['error'].get('code', 'Unknown')} - {error_details['error'].get('message', 'No details')}"
                except:
                    error_msg += "Likely causes: Missing Graph permissions, conditional access policies, or security defaults."
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg, response=response)
            
            elif response.status_code == 503:
                error_msg = f"503 Service Unavailable - Tenant {self.tenant_id}: Microsoft Graph service temporarily unavailable."
                logging.warning(error_msg + " Retrying after delay...")
                time.sleep(30)  # Wait longer for service issues
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