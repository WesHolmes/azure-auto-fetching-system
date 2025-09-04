import logging
import os
import time

import msal
import requests


# Note: time.sleep() is acceptable here because:
# 1. Azure Functions handles scaling automatically
# 2. These are legitimate API rate limits that must be respected
# 3. The GraphBetaClient is synchronous by design.


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

        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

        if "access_token" in result:
            self.token = result["access_token"]
            self.token_expires = time.time() + result.get("expires_in", 3600) - 300
            return self.token
        else:
            raise Exception(f"Token acquisition failed: {result.get('error', 'Unknown error')}")

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
                retry_count = getattr(self, "_retry_count", 0)
                self._retry_count = retry_count + 1
                wait_time = min(30, 5 * (2**retry_count))  # Max 30 seconds
                logging.info(f"Waiting {wait_time} seconds before retry #{self._retry_count}")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            data = response.json()

            results = data.get("value", [])
            all_results.extend(results)

            if top and len(all_results) >= top:
                return all_results[:top]

            url = data.get("@odata.nextLink")

        return all_results

    def patch_user(self, user_id, update_data):
        headers = {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}/users/{user_id}"
        response = requests.patch(url, headers=headers, json=update_data)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            logging.warning(f"Rate limited - waiting {retry_after} seconds")
            time.sleep(retry_after)
            response = requests.patch(url, headers=headers, json=update_data)

        response.raise_for_status()
        return response.json() if response.content else {}

    def get_tenant_details(self, tenant_id):
        data = self.get(f"/organization/{tenant_id}")
        return data

    def create_user(self, user_data):
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }

            url = f"{self.base_url}/users"

            graph_user_data = {
                "accountEnabled": user_data.get("accountEnabled", True),
                "displayName": user_data.get("displayName"),
                "mailNickname": user_data.get("mailNickname"),
                "passwordProfile": user_data.get("passwordProfile"),
                "userPrincipalName": user_data.get("userPrincipalName"),
                "usageLocation": user_data.get("usageLocation", "US"),
                "department": user_data.get("department"),
                "jobTitle": user_data.get("jobTitle"),
                "officeLocation": user_data.get("officeLocation"),
                "mobilePhone": user_data.get("mobilePhone"),
            }

            graph_user_data = {k: v for k, v in graph_user_data.items() if v is not None}

            logging.info(f"Creating user in tenant: {self.tenant_id}")
            logging.info(f"Graph Beta API URL: {url}")
            logging.info(f"Original user data: {user_data}")
            logging.info(f"Filtered Graph API data: {graph_user_data}")

            response = requests.post(url, headers=headers, json=graph_user_data)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while creating user - waiting {retry_after} seconds")
                time.sleep(retry_after)
                response = requests.post(url, headers=headers, json=user_data)

            if response.status_code == 401:
                error_msg = "401 Unauthorized - Cannot create user: Authentication failed"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 403:
                error_msg = "403 Forbidden - Cannot create user: Insufficient permissions"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 409:
                error_msg = "409 Conflict - User already exists or duplicate userPrincipalName"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 400:
                try:
                    error_details = response.json()
                    error_msg = f"400 Bad Request - Invalid user data: {error_details.get('error', {}).get('message', 'Unknown error')}"
                except Exception:
                    error_msg = "400 Bad Request - Invalid user data"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 503:
                error_msg = "503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}

            if response.status_code == 201:
                created_user = response.json()
                logging.info(f"Successfully created user {created_user.get('userPrincipalName', 'Unknown')}")

                return {
                    "status": "success",
                    "message": f"User {created_user.get('userPrincipalName', 'Unknown')} created successfully",
                    "data": created_user,
                }

            logging.error(f"Graph Beta API returned status code: {response.status_code}")
            logging.error(f"Response headers: {dict(response.headers)}")
            try:
                error_details = response.json()
                logging.error(f"Response body: {error_details}")
            except Exception:
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
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }

            url = f"{self.base_url}/users/{user_id}"

            logging.info(f"Deleting user {user_id} in tenant: {self.tenant_id}")
            logging.info(f"Graph Beta API URL: {url}")

            response = requests.delete(url, headers=headers)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while deleting user - waiting {retry_after} seconds")
                time.sleep(retry_after)
                response = requests.delete(url, headers=headers)

            if response.status_code == 401:
                error_msg = "401 Unauthorized - Cannot delete user: Authentication failed"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 403:
                error_msg = "403 Forbidden - Cannot delete user: Insufficient permissions"
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
                except Exception:
                    error_msg = "400 Bad Request - Invalid request"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 503:
                error_msg = "503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}

            if response.status_code == 204:
                logging.info(f"Successfully deleted user {user_id}")
                return {"status": "success", "message": f"User {user_id} deleted successfully"}

            logging.error(f"Graph Beta API returned status code: {response.status_code}")
            logging.error(f"Response headers: {dict(response.headers)}")
            try:
                error_details = response.json()
                logging.error(f"Response body: {error_details}")
            except Exception:
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

    def update_user(self, user_id, user_updates):
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }

            url = f"{self.base_url}/users/{user_id}"

            logging.info(f"Updating user {user_id} in tenant: {self.tenant_id}")
            logging.info(f"Graph Beta API URL: {url}")
            logging.info(f"Update data: {user_updates}")

            response = requests.patch(url, headers=headers, json=user_updates)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while updating user - waiting {retry_after} seconds")
                time.sleep(retry_after)
                response = requests.patch(url, headers=headers, json=user_updates)

            if response.status_code == 401:
                error_msg = "401 Unauthorized - Cannot update user: Authentication failed"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 403:
                error_msg = "403 Forbidden - Cannot update user: Insufficient permissions"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 404:
                error_msg = f"404 Not Found - User {user_id} not found"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 400:
                try:
                    error_details = response.json()
                    error_msg = f"400 Bad Request - Invalid update data: {error_details.get('error', {}).get('message', 'Unknown error')}"
                except Exception:
                    error_msg = "400 Bad Request - Invalid update data"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 503:
                error_msg = "503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}

            if response.status_code == 200:
                updated_user = response.json()
                logging.info(f"Successfully updated user {user_id}")
                return {"status": "success", "message": f"User {user_id} updated successfully", "data": updated_user}

            logging.error(f"Graph Beta API returned status code: {response.status_code}")
            logging.error(f"Response headers: {dict(response.headers)}")
            try:
                error_details = response.json()
                logging.error(f"Response body: {error_details}")
            except Exception:
                logging.error(f"Response text: {response.text}")

            response.raise_for_status()
            return {"status": "success", "message": "User updated successfully"}

        except requests.exceptions.RequestException as e:
            error_msg = f"Network error while updating user: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}

        except Exception as e:
            error_msg = f"Unexpected error while updating user: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}

    def assign_role(self, user_id, role_name):
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }

            role_url = f"{self.base_url}/directoryRoleTemplates"
            logging.info(f"Fetching role template for '{role_name}'")

            role_response = requests.get(role_url, headers=headers)
            if role_response.status_code != 200:
                error_msg = f"Failed to fetch role templates: HTTP {role_response.status_code}"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            role_templates = role_response.json().get("value", [])
            target_role = None

            for role in role_templates:
                if role.get("displayName", "").lower() == role_name.lower():
                    target_role = role
                    break

            if not target_role:
                error_msg = f"Role '{role_name}' not found in available roles"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            role_template_id = target_role.get("id")
            logging.info(f"Found role template ID: {role_template_id} for role: {role_name}")

            activated_roles_url = f"{self.base_url}/directoryRoles"
            activated_response = requests.get(activated_roles_url, headers=headers)

            if activated_response.status_code == 200:
                activated_roles = activated_response.json().get("value", [])
                role_exists = any(role.get("roleTemplateId") == role_template_id for role in activated_roles)

                if not role_exists:
                    logging.info(f"Role '{role_name}' not activated in tenant. Activating...")
                    activate_data = {"roleTemplateId": role_template_id}
                    activate_url = f"{self.base_url}/directoryRoles"
                    activate_response = requests.post(activate_url, headers=headers, json=activate_data)

                    if activate_response.status_code not in [200, 201]:
                        error_msg = f"Failed to activate role '{role_name}' in tenant: HTTP {activate_response.status_code}"
                        logging.error(error_msg)
                        return {"status": "error", "error": error_msg}

                    logging.info(f"Successfully activated role '{role_name}' in tenant")
                    activated_response = requests.get(activated_roles_url, headers=headers)
                    if activated_response.status_code == 200:
                        activated_roles = activated_response.json().get("value", [])
                        activated_role = next((role for role in activated_roles if role.get("roleTemplateId") == role_template_id), None)
                        if activated_role:
                            role_template_id = activated_role.get("id")
                            logging.info(f"Using activated role ID: {role_template_id}")
                        else:
                            error_msg = f"Failed to retrieve activated role ID for '{role_name}' after activation"
                            logging.error(error_msg)
                            return {"status": "error", "error": error_msg}
                    else:
                        error_msg = f"Failed to retrieve activated roles after activation: HTTP {activated_response.status_code}"
                        logging.error(error_msg)
                        return {"status": "error", "error": error_msg}
                else:
                    existing_role = next((role for role in activated_roles if role.get("roleTemplateId") == role_template_id), None)
                    if existing_role:
                        role_template_id = existing_role.get("id")
                        logging.info(f"Using existing activated role ID: {role_template_id}")
                    else:
                        error_msg = f"Failed to retrieve existing role ID for '{role_name}'"
                        logging.error(error_msg)
                        return {"status": "error", "error": error_msg}

            assignment_url = f"{self.base_url}/directoryRoles/{role_template_id}/members/$ref"

            assignment_data = {"@odata.id": f"{self.base_url}/users/{user_id}"}

            logging.info(
                f"Assigning role '{role_name}' to user {user_id}, using activated role ID: {role_template_id}, assignment URL: {assignment_url}, assignment data: {assignment_data}"
            )

            assignment_response = requests.post(assignment_url, headers=headers, json=assignment_data)

            if assignment_response.status_code == 204:
                logging.info(f"Successfully assigned role '{role_name}' to user {user_id}")
                return {"status": "success", "message": f"Role '{role_name}' assigned successfully"}
            else:
                error_msg = f"Failed to assign role: HTTP {assignment_response.status_code}"
                try:
                    error_details = assignment_response.json()
                    error_msg += f" - {error_details.get('error', {}).get('message', 'Unknown error')}"
                except Exception:
                    pass
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

        except Exception as e:
            error_msg = f"Failed to assign role: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}

    def assign_license(self, user_id, license_sku):
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }

            licenses_url = f"{self.base_url}/subscribedSkus"
            logging.info(f"Fetching available licenses for tenant {self.tenant_id}")

            licenses_response = requests.get(licenses_url, headers=headers)
            if licenses_response.status_code != 200:
                error_msg = f"Failed to fetch tenant licenses: HTTP {licenses_response.status_code}"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            tenant_licenses = licenses_response.json().get("value", [])
            target_license = None

            for license_info in tenant_licenses:
                sku_id = license_info.get("skuId")
                sku_part_number = license_info.get("skuPartNumber", "")
                display_name = license_info.get("capabilityStatus", "")

                if (
                    license_sku.lower() == sku_id.lower()
                    or license_sku.lower() == sku_part_number.lower()
                    or license_sku.lower() in display_name.lower()
                ):
                    target_license = license_info
                    break

            if not target_license:
                available_licenses = [license_item.get("skuPartNumber", license_item.get("skuId")) for license_item in tenant_licenses]
                error_msg = f"License '{license_sku}' not found in tenant. Available licenses: {available_licenses}"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            actual_sku_id = target_license.get("skuId")
            logging.info(f"Found license: {target_license.get('skuPartNumber', actual_sku_id)} (SKU ID: {actual_sku_id})")

            url = f"{self.base_url}/users/{user_id}/assignLicense"

            license_data = {
                "addLicenses": [
                    {
                        "disabledPlans": [],
                        "skuId": actual_sku_id,
                    }
                ],
                "removeLicenses": [],
            }

            logging.info(f"Assigning license '{license_sku}' (SKU: {actual_sku_id}) to user {user_id}")
            logging.info(f"License assignment URL: {url}")
            logging.info(f"License data: {license_data}")

            response = requests.post(url, headers=headers, json=license_data)

            if response.status_code == 200:
                result = response.json()
                logging.info(f"Successfully assigned license '{license_sku}' to user {user_id}")
                return {"status": "success", "data": result}
            else:
                error_msg = f"Failed to assign license: HTTP {response.status_code}"
                try:
                    error_details = response.json()
                    error_msg += f" - {error_details.get('error', {}).get('message', 'Unknown error')}"
                except Exception:
                    pass
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

        except Exception as e:
            error_msg = f"Failed to assign license: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}

    def disable_user(self, user_id):
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }

            url = f"{self.base_url}/users/{user_id}"

            data = {"accountEnabled": False}

            response = requests.patch(url, headers=headers, json=data)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while disabling user - waiting {retry_after} seconds")
                time.sleep(retry_after)
                response = requests.patch(url, headers=headers, json=data)

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
                error_msg = "503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}

            if response.status_code in [200, 204]:
                logging.info(f"Successfully disabled user {user_id}")
                return {"status": "success", "message": f"User {user_id} disabled successfully"}

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

    def reset_user_password(self, user_id):
        try:
            headers = {
                "Authorization": f"Bearer {self.get_token()}",
                "Content-Type": "application/json",
            }

            import secrets
            import string

            temp_password = "".join(secrets.choice(string.ascii_letters + string.digits + "!@#$%&*") for _ in range(12))

            url = f"{self.base_url}/users/{user_id}"

            data = {"passwordProfile": {"password": temp_password, "forceChangePasswordNextSignIn": True}}

            response = requests.patch(url, headers=headers, json=data)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limited while resetting password - waiting {retry_after} seconds")
                time.sleep(retry_after)
                response = requests.patch(url, headers=headers, json=data)

            if response.status_code == 401:
                error_msg = f"401 Unauthorized - Cannot reset password for user {user_id}: Authentication failed"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 403:
                error_msg = f"403 Forbidden - Cannot reset password for user {user_id}: Insufficient permissions"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 404:
                error_msg = f"404 Not Found - User {user_id} does not exist"
                logging.error(error_msg)
                return {"status": "error", "error": error_msg}

            elif response.status_code == 503:
                error_msg = "503 Service Unavailable - Microsoft Graph service temporarily unavailable"
                logging.warning(error_msg)
                return {"status": "error", "error": error_msg}

            if response.status_code in [200, 204]:
                logging.info(f"Successfully reset password for user {user_id}")
                return {"status": "success", "message": f"Password reset for user {user_id}", "temporary_password": temp_password}

            response.raise_for_status()
            return {"status": "success", "message": f"Password reset for user {user_id}", "temporary_password": temp_password}

        except requests.exceptions.RequestException as e:
            error_msg = f"Network error while resetting password for user {user_id}: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}

        except Exception as e:
            error_msg = f"Unexpected error while resetting password for user {user_id}: {str(e)}"
            logging.error(error_msg)
            return {"status": "error", "error": error_msg}
