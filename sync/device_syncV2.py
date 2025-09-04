from datetime import datetime
import logging

from core.graph_beta_client import GraphBetaClient
from core.graph_client import GraphClient
from sql.databaseV2 import init_schema, upsert_many
from utils.http import clean_error_message


logger = logging.getLogger(__name__)

""" Intune additional $select props: manufacturer, totalStorageSpaceInBytes,
freeStorageSpaceInBytes, physicalMemoryInBytes, isEncrypted, complianceState"""


def format_bytes(bytes_value):
    """Convert bytes to readable format (B, KB, MB, GB, TB)"""
    if bytes_value is None or bytes_value == 0:
        return "N/A"

    try:
        bytes_value = int(bytes_value)
        if bytes_value < 0:
            return "N/A"
    except (ValueError, TypeError):
        return "N/A"

    # Define units and their byte values
    units = [("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024), ("B", 1)]

    # Find the appropriate unit
    for unit, size in units:
        if bytes_value >= size:
            value = bytes_value / size
            # Round to 1 decimal place, but show as integer if it's a whole number
            if value == int(value):
                return f"{int(value)} {unit}"
            else:
                return f"{value:.1f} {unit}"

    return "0 B"


def fetch_intune_devices(tenant_id):
    """Fetch Intune managed devices from Graph API Beta endpoint"""
    try:
        logger.info(f"Starting Intune device fetch for tenant {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        devices = graph.get(
            "/deviceManagement/managedDevices",
            select=[
                "id",
                "deviceName",
                "enrolledDateTime",
                "lastSyncDateTime",
                "operatingSystem",
                "osVersion",
                "model",
                "serialNumber",
                "managedDeviceOwnerType",
                "isEncrypted",
                "azureADRegistered",
                "userId",  # Add userId field for user-device relationship
                "manufacturer",  # Additional Intune fields
                "totalStorageSpaceInBytes",
                "freeStorageSpaceInBytes",
                "physicalMemoryInBytes",
                "complianceState",
            ],
            top=999,
        )

        logger.info(f"Successfully fetched {len(devices)} Intune devices for tenant {tenant_id}")
        return devices

    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch Intune devices")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise


def fetch_azure_devices(tenant_id):
    """Fetch Azure AD devices from Graph API v1.0 endpoint"""
    try:
        logger.info(f"Starting Azure device fetch for tenant {tenant_id}")
        graph = GraphClient(tenant_id)

        devices = graph.get(
            "/devices",
            select=[
                "id",
                "displayName",
                "deviceOwnership",
                "isCompliant",
                "isManaged",
                "managementType",
                "manufacturer",  # Already included for Azure devices
                "model",
                "serialNumber",
                "operatingSystem",
                "operatingSystemVersion",
                "approximateLastSignInDateTime",
            ],
            top=999,
        )

        logger.info(f"Successfully fetched {len(devices)} Azure devices for tenant {tenant_id}")
        return devices

    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch Azure devices")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        raise


def fetch_azure_device_registered_users(tenant_id, device_id):
    """Fetch registered users for a specific Azure device"""
    try:
        graph = GraphClient(tenant_id)
        users = graph.get(f"/devices/{device_id}/registeredUsers", select=["id", "userPrincipalName"])
        return users
    except Exception as e:
        logger.debug(f"Failed to fetch registered users for Azure device {device_id}: {str(e)}")
        return []


def _test_tenant_capability(graph, graph_beta, tenant_id):
    """Helper function to test tenant capability for premium features"""
    try:
        # Test with a single user to check if signin activity is accessible
        test_user = graph.get("/users", select=["id", "userPrincipalName"], top=1)
        if not test_user:
            logger.warning(f"No users found in tenant {tenant_id} for capability testing")
            return False

        # Try to fetch signin activity for the first user (BETA endpoint)
        user_id = test_user[0]["id"]
        try:
            graph_beta.get(f"/users/{user_id}/signInActivity", select=["lastSignInDateTime"])
            logger.info(f"Tenant {tenant_id} is PREMIUM - beta signin activity accessible")
            return True
        except Exception:
            # Fallback: test if we can access beta MFA data (another beta-only feature)
            try:
                # Test if we can access MFA registration details (beta endpoint)
                graph_beta.get("/reports/authenticationMethods/userRegistrationDetails", select=["id"], top=1)
                logger.info(f"Tenant {tenant_id} is PREMIUM - beta MFA data accessible")
                return True
            except Exception:
                logger.info(f"Tenant {tenant_id} is NOT PREMIUM - no beta features accessible")
                return False
    except Exception as capability_error:
        logger.warning(f"Could not determine tenant capability for {tenant_id}: {str(capability_error)}")
        return False


def transform_intune_devices(devices, tenant_id):
    """Transform Intune devices to database records"""
    records = []

    for device in devices:
        try:
            device_id = f"intune_{device.get('id')}"
            device_name = device.get("deviceName") or "N/A"
            model = device.get("model") or "N/A"
            serial_number = device.get("serialNumber") or "N/A"
            operating_system = device.get("operatingSystem") or "N/A"
            os_version = device.get("osVersion") or "N/A"

            # Map device ownership
            owner_type = device.get("managedDeviceOwnerType", "")
            if owner_type == "company":
                device_ownership = "corporate"
            elif owner_type == "personal":
                device_ownership = "personal"
            else:
                device_ownership = "N/A"

            # Intune devices now have compliance fields
            compliance_state = device.get("complianceState", "unknown")
            is_compliant = 1 if compliance_state == "compliant" else 0
            is_managed = 1  # Intune devices are managed by definition

            # Handle storage and memory fields
            manufacturer = device.get("manufacturer") or "N/A"
            total_storage = format_bytes(device.get("totalStorageSpaceInBytes"))
            free_storage = format_bytes(device.get("freeStorageSpaceInBytes"))
            physical_memory = format_bytes(device.get("physicalMemoryInBytes"))
            is_encrypted = 1 if device.get("isEncrypted") else 0

            # Handle dates
            enrolled_date = device.get("enrolledDateTime")
            last_sign_in_date = device.get("lastSyncDateTime")  # Use last sync as proxy for activity

            record = {
                "tenant_id": tenant_id,
                "device_id": device_id,
                "device_type": "Intune",
                "device_name": device_name,
                "model": model,
                "serial_number": serial_number,
                "operating_system": operating_system,
                "os_version": os_version,
                "device_ownership": device_ownership,
                "is_compliant": is_compliant,
                "is_managed": is_managed,
                "manufacturer": manufacturer,
                "total_storage": total_storage,
                "free_storage": free_storage,
                "physical_memory": physical_memory,
                "compliance_state": compliance_state,
                "is_encrypted": is_encrypted,
                "last_sign_in_date": last_sign_in_date,
                "enrolled_date": enrolled_date,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                # Store user_id for relationship creation
                "_user_id": device.get("userId"),
            }
            records.append(record)

        except Exception as e:
            logger.error(f"Failed to process Intune device {device.get('deviceName', 'Unknown')}: {str(e)}")
            continue

    return records


def transform_azure_devices(devices, tenant_id):
    """Transform Azure devices to database records"""
    records = []

    for device in devices:
        try:
            device_id = f"azure_{device.get('id')}"
            device_name = device.get("displayName") or "N/A"
            model = device.get("model") or "N/A"
            serial_number = device.get("serialNumber") or "N/A"
            operating_system = device.get("operatingSystem") or "N/A"
            os_version = device.get("operatingSystemVersion") or "N/A"

            # Map device ownership
            ownership = device.get("deviceOwnership", "")
            if ownership == "Company":
                device_ownership = "corporate"
            elif ownership == "Personal":
                device_ownership = "personal"
            else:
                device_ownership = "N/A"

            # Azure devices have compliance fields
            is_compliant = 1 if device.get("isCompliant") else 0
            is_managed = 1 if device.get("isManaged") else 0

            # Handle additional fields for Azure devices
            manufacturer = device.get("manufacturer") or "N/A"
            # Azure devices don't have storage/memory/compliance state fields
            total_storage = "N/A"
            free_storage = "N/A"
            physical_memory = "N/A"
            compliance_state = "unknown"
            is_encrypted = 0  # Azure devices don't have encryption info in this endpoint

            # Handle dates
            last_sign_in_date = device.get("approximateLastSignInDateTime")
            enrolled_date = None  # Azure devices don't have enrollment date in this endpoint

            record = {
                "tenant_id": tenant_id,
                "device_id": device_id,
                "device_type": "Azure",
                "device_name": device_name,
                "model": model,
                "serial_number": serial_number,
                "operating_system": operating_system,
                "os_version": os_version,
                "device_ownership": device_ownership,
                "is_compliant": is_compliant,
                "is_managed": is_managed,
                "manufacturer": manufacturer,
                "total_storage": total_storage,
                "free_storage": free_storage,
                "physical_memory": physical_memory,
                "compliance_state": compliance_state,
                "is_encrypted": is_encrypted,
                "last_sign_in_date": last_sign_in_date,
                "enrolled_date": enrolled_date,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                # Store original device ID for fetching registered users
                "_original_device_id": device.get("id"),
            }
            records.append(record)

        except Exception as e:
            logger.error(f"Failed to process Azure device {device.get('displayName', 'Unknown')}: {str(e)}")
            continue

    return records


def create_user_device_relationships(devices, tenant_id):
    """Create user-device relationship records"""
    relationships = []

    for device in devices:
        try:
            device_id = device["device_id"]
            device_type = device["device_type"]

            if device_type == "Intune":
                # For Intune devices, use the userId from the device record
                user_id = device.get("_user_id")
                if user_id:
                    relationship = {
                        "user_id": user_id,
                        "tenant_id": tenant_id,
                        "device_id": device_id,
                        "relationship_type": "owner",
                        "created_at": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat(),
                    }
                    relationships.append(relationship)
                else:
                    logger.debug(f"No user_id found for Intune device {device_id}")

            elif device_type == "Azure":
                # For Azure devices, fetch registered users
                original_device_id = device.get("_original_device_id")
                if original_device_id:
                    try:
                        registered_users = fetch_azure_device_registered_users(tenant_id, original_device_id)
                        for user in registered_users:
                            relationship = {
                                "user_id": user["id"],
                                "tenant_id": tenant_id,
                                "device_id": device_id,
                                "relationship_type": "registered_user",
                                "created_at": datetime.now().isoformat(),
                                "last_updated": datetime.now().isoformat(),
                            }
                            relationships.append(relationship)
                    except Exception as e:
                        logger.debug(f"Failed to fetch registered users for Azure device {device_id}: {str(e)}")
                else:
                    logger.debug(f"No original_device_id found for Azure device {device_id}")

        except Exception as e:
            logger.error(f"Failed to create relationship for device {device.get('device_id', 'Unknown')}: {str(e)}")
            continue

    logger.info(f"Created {len(relationships)} user-device relationships for {len(devices)} devices")
    return relationships


def sync_devices(tenant_id, tenant_name):
    """Orchestrate device synchronization"""
    start_time = datetime.now()
    logger.info(f"Starting device sync for {tenant_name} (tenant_id: {tenant_id})")

    # Initialize database schema
    init_schema()

    try:
        # Test tenant capability for premium features
        graph = GraphClient(tenant_id)
        graph_beta = GraphBetaClient(tenant_id)
        is_premium = _test_tenant_capability(graph, graph_beta, tenant_id)

        all_device_records = []
        all_relationship_records = []

        # Sync Intune devices (premium tenants only)
        if is_premium:
            try:
                logger.info(f"Syncing Intune devices for premium tenant {tenant_name}")
                intune_devices = fetch_intune_devices(tenant_id)
                intune_records = transform_intune_devices(intune_devices, tenant_id)
                all_device_records.extend(intune_records)

                # Create user-device relationships for Intune devices
                intune_relationships = create_user_device_relationships(intune_records, tenant_id)
                all_relationship_records.extend(intune_relationships)

                logger.info(f"Processed {len(intune_records)} Intune devices")

            except Exception as e:
                logger.error(f"Failed to sync Intune devices for {tenant_name}: {str(e)}")
                # Continue with Azure sync even if Intune fails

        # Sync Azure devices (all tenants)
        try:
            logger.info(f"Syncing Azure devices for tenant {tenant_name}")
            azure_devices = fetch_azure_devices(tenant_id)
            azure_records = transform_azure_devices(azure_devices, tenant_id)
            all_device_records.extend(azure_records)

            # Create user-device relationships for Azure devices
            azure_relationships = create_user_device_relationships(azure_records, tenant_id)
            all_relationship_records.extend(azure_relationships)

            logger.info(f"Processed {len(azure_records)} Azure devices")

        except Exception as e:
            logger.error(f"Failed to sync Azure devices for {tenant_name}: {str(e)}")
            # If both fail, we'll have an empty result

        if not all_device_records:
            logger.warning(f"No devices found for {tenant_name}")
            return {
                "status": "success",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "devices_synced": 0,
                "relationships_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # Clean device records (remove temporary fields used for relationship creation)
        clean_device_records = []
        for record in all_device_records:
            clean_record = {k: v for k, v in record.items() if not k.startswith("_")}
            clean_device_records.append(clean_record)

        # Store devices in database
        devices_stored = 0
        relationships_stored = 0

        try:
            if clean_device_records:
                devices_stored = upsert_many("devices", clean_device_records)
                logger.info(f"Stored {devices_stored} devices for {tenant_name}")

            if all_relationship_records:
                relationships_stored = upsert_many("user_devicesV2", all_relationship_records)
                logger.info(f"Stored {relationships_stored} user-device relationships for {tenant_name}")
            else:
                logger.info(f"No user-device relationships to store for {tenant_name}")

        except Exception as e:
            logger.error(f"Failed to store devices for {tenant_name}: {str(e)}", exc_info=True)
            raise

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Completed device sync for {tenant_name}: {devices_stored} devices, {relationships_stored} relationships in {duration:.1f}s"
        )

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "devices_synced": devices_stored,
            "relationships_synced": relationships_stored,
            "duration_seconds": duration,
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = clean_error_message(str(e), tenant_name=tenant_name)
        logger.error(error_msg)
        logger.debug(f"Full error details for {tenant_name}: {str(e)}", exc_info=True)

        return {
            "status": "error",
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "error": str(e),
            "duration_seconds": duration,
        }
