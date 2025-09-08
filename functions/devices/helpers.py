from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from db.db_client import init_schema, upsert_many
from shared.graph_beta_client import GraphBetaClient
from shared.graph_client import GraphClient
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


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


def bytes_to_gb(bytes_value):
    """Convert bytes to GB as a float for database storage and sorting"""
    if bytes_value is None or bytes_value == 0:
        return None

    try:
        bytes_value = int(bytes_value)
        if bytes_value < 0:
            return None
        # Convert to GB
        return round(bytes_value / (1024**3), 2)
    except (ValueError, TypeError):
        return None


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


def fetch_intune_devices(tenant_id):
    """Fetch Intune managed devices from Graph API Beta endpoint"""
    try:
        logger.info(f"Starting Intune device fetch for tenant {tenant_id}")
        graph = GraphBetaClient(tenant_id)

        # First, get the list of devices
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
                "complianceState",
            ],
            top=999,
        )

        logger.info(f"Successfully fetched {len(devices)} Intune devices for tenant {tenant_id}")

        # Now fetch physical memory for each device individually
        # This is required because physicalMemoryInBytes only returns real values in individual GET calls
        logger.info(f"Fetching physical memory data for {len(devices)} devices...")
        for i, device in enumerate(devices):
            try:
                device_id = device.get("id")
                if device_id:
                    # Make individual GET request for hardware information
                    # Microsoft Graph API has a known issue where physicalMemoryInBytes returns 0
                    # We need to use the hardwareInformation endpoint instead
                    device_details = graph.get(f"/deviceManagement/managedDevices/{device_id}", select=["hardwareInformation"])

                    # Debug: Log the response type and content for troubleshooting
                    logger.debug(f"Device {device.get('deviceName', 'Unknown')} response type: {type(device_details)}")
                    logger.debug(f"Device {device.get('deviceName', 'Unknown')} response content: {device_details}")

                    # Handle different response types - sometimes Graph API returns a list
                    if isinstance(device_details, list):
                        if device_details and len(device_details) > 0:
                            device_details = device_details[0]  # Take the first item if it's a list
                        else:
                            device_details = {}
                    elif not isinstance(device_details, dict):
                        logger.warning(f"Unexpected response type for device {device.get('deviceName', 'Unknown')}: {type(device_details)}")
                        device_details = {}

                    # Extract physical memory from hardwareInformation
                    hardware_info = device_details.get("hardwareInformation", {}) if device_details else {}
                    physical_memory = hardware_info.get("totalMemoryInBytes") if hardware_info else None
                    device["physicalMemoryInBytes"] = physical_memory

                    # Log the physical memory value for debugging
                    logger.debug(f"Device {device.get('deviceName', 'Unknown')} physical memory: {physical_memory}")

                    if (i + 1) % 50 == 0:  # Log progress every 50 devices
                        logger.info(f"Processed physical memory for {i + 1}/{len(devices)} devices")

            except Exception as e:
                logger.warning(f"Failed to fetch physical memory for device {device.get('deviceName', 'Unknown')}: {str(e)}")
                device["physicalMemoryInBytes"] = None
                continue

        logger.info("Completed fetching physical memory data for all devices")

        # Debug: Log sample device data to see what fields are available
        if devices:
            sample_device = devices[0]
            logger.debug(f"Sample Intune device fields: {list(sample_device.keys())}")
            logger.debug(
                f"Sample device storage fields: totalStorageSpaceInBytes={sample_device.get('totalStorageSpaceInBytes')}, freeStorageSpaceInBytes={sample_device.get('freeStorageSpaceInBytes')}, physicalMemoryInBytes={sample_device.get('physicalMemoryInBytes')}, isEncrypted={sample_device.get('isEncrypted')}"
            )

        return devices

    except Exception as e:
        error_msg = clean_error_message(str(e), "Failed to fetch Intune devices")
        logger.error(error_msg)
        logger.debug(f"Full error details for tenant {tenant_id}: {str(e)}", exc_info=True)
        return []


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
        return []


def fetch_azure_device_registered_users(tenant_id, device_id):
    """Fetch registered users for a specific Azure device"""
    try:
        graph = GraphClient(tenant_id)
        users = graph.get(f"/devices/{device_id}/registeredUsers", select=["id", "userPrincipalName"])
        return users
    except Exception as e:
        logger.debug(f"Failed to fetch registered users for Azure device {device_id}: {str(e)}")
        return []


def transform_intune_devices(devices, tenant_id):
    """Transform Intune devices to database records for intune_devices table"""
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

            # Get raw byte values
            total_storage_bytes = device.get("totalStorageSpaceInBytes")
            free_storage_bytes = device.get("freeStorageSpaceInBytes")
            physical_memory_bytes = device.get("physicalMemoryInBytes")
            is_encrypted_raw = device.get("isEncrypted")

            # Debug logging for storage fields
            logger.debug(
                f"Device {device_name} storage data: total={total_storage_bytes}, free={free_storage_bytes}, memory={physical_memory_bytes}, encrypted={is_encrypted_raw}"
            )

            # Convert to GB for proper database sorting
            total_storage_gb = bytes_to_gb(total_storage_bytes)
            free_storage_gb = bytes_to_gb(free_storage_bytes)
            physical_memory_gb = bytes_to_gb(physical_memory_bytes)
            is_encrypted = 1 if is_encrypted_raw else 0

            # Handle dates - ensure proper ISO format
            enrolled_date = device.get("enrolledDateTime")
            last_sign_in_date = device.get("lastSyncDateTime")  # Use last sync as proxy for activity

            # Convert dates to proper ISO format if they exist
            if enrolled_date and not enrolled_date.endswith("Z"):
                # Ensure proper ISO format
                try:
                    from datetime import datetime

                    if "T" in enrolled_date:
                        enrolled_date = enrolled_date + "Z" if not enrolled_date.endswith("Z") else enrolled_date
                except Exception:
                    enrolled_date = None

            if last_sign_in_date and not last_sign_in_date.endswith("Z"):
                try:
                    from datetime import datetime

                    if "T" in last_sign_in_date:
                        last_sign_in_date = last_sign_in_date + "Z" if not last_sign_in_date.endswith("Z") else last_sign_in_date
                except Exception:
                    last_sign_in_date = None

            record = {
                "tenant_id": tenant_id,
                "device_id": device_id,
                "device_name": device_name,
                "model": model,
                "serial_number": serial_number,
                "operating_system": operating_system,
                "os_version": os_version,
                "device_ownership": device_ownership,
                "is_compliant": is_compliant,
                "is_managed": is_managed,
                "manufacturer": manufacturer,
                "total_storage_gb": total_storage_gb,
                "free_storage_gb": free_storage_gb,
                "physical_memory_gb": physical_memory_gb,
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


def create_user_device_relationships_batch(tenant_id, devices):
    """Create user-device relationship records with concurrent processing"""
    relationships = []

    def process_device_relationships(device):
        """Process relationships for a single device"""
        try:
            device_id = device["device_id"]
            device_type = device["device_type"]

            if device_type == "Intune":
                # For Intune devices, use the userId from the device record
                user_id = device.get("_user_id")
                if user_id:
                    return [
                        {
                            "user_id": user_id,
                            "tenant_id": tenant_id,
                            "device_id": device_id,
                            "relationship_type": "owner",
                            "created_at": datetime.now().isoformat(),
                            "last_updated": datetime.now().isoformat(),
                        }
                    ]
                else:
                    logger.debug(f"No user_id found for Intune device {device_id}")
                    return []

            elif device_type == "Azure":
                # For Azure devices, fetch registered users
                original_device_id = device.get("_original_device_id")
                if original_device_id:
                    try:
                        registered_users = fetch_azure_device_registered_users(tenant_id, original_device_id)
                        device_relationships = []
                        for user in registered_users:
                            relationship = {
                                "user_id": user["id"],
                                "tenant_id": tenant_id,
                                "device_id": device_id,
                                "relationship_type": "registered_user",
                                "created_at": datetime.now().isoformat(),
                                "last_updated": datetime.now().isoformat(),
                            }
                            device_relationships.append(relationship)
                        return device_relationships
                    except Exception as e:
                        logger.debug(f"Failed to fetch registered users for Azure device {device_id}: {str(e)}")
                        return []
                else:
                    logger.debug(f"No original_device_id found for Azure device {device_id}")
                    return []

        except Exception as e:
            logger.error(f"Failed to create relationship for device {device.get('device_id', 'Unknown')}: {str(e)}")
            return []

    # Use ThreadPoolExecutor for concurrent relationship processing
    with ThreadPoolExecutor(max_workers=15) as executor:
        future_to_device = {executor.submit(process_device_relationships, device): device for device in devices}

        processed_count = 0
        total_devices = len(devices)

        for future in as_completed(future_to_device):
            device = future_to_device[future]
            try:
                device_relationships = future.result()
                relationships.extend(device_relationships)
            except Exception as e:
                logger.warning(f"Failed to process relationships for device {device.get('device_id', 'unknown')}: {str(e)}")
                continue

            processed_count += 1
            if processed_count % 50 == 0 or processed_count == total_devices:
                logger.info(f"Processed {processed_count}/{total_devices} device relationships...")

    logger.info(f"Created {len(relationships)} user-device relationships for {len(devices)} devices")
    return relationships


def sync_devices(tenant_id, tenant_name):
    """Orchestrate device synchronization with concurrent processing"""
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
                if intune_devices:
                    intune_records = transform_intune_devices(intune_devices, tenant_id)
                    all_device_records.extend(intune_records)
                    logger.info(f"Processed {len(intune_records)} Intune devices")
                else:
                    logger.info(f"No Intune devices found for {tenant_name}")

            except Exception as e:
                logger.error(f"Failed to sync Intune devices for {tenant_name}: {str(e)}")
                # Continue with Azure sync even if Intune fails

        # Note: Azure devices are now handled separately - this function only handles Intune devices

        if not all_device_records:
            logger.warning(f"No devices found for {tenant_name}")
            return {
                "status": "completed",
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "devices_synced": 0,
                "relationships_synced": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

        # Create user-device relationships with concurrent processing
        logger.info(f"Creating user-device relationships for {len(all_device_records)} devices...")
        all_relationship_records = create_user_device_relationships_batch(tenant_id, all_device_records)

        # Clean device records (remove temporary fields used for relationship creation)
        clean_device_records = []
        for record in all_device_records:
            clean_record = {k: v for k, v in record.items() if not k.startswith("_")}
            clean_device_records.append(clean_record)

        # Store devices and relationships in database
        devices_stored = 0
        relationships_stored = 0

        try:
            if clean_device_records:
                devices_stored = upsert_many("intune_devices", clean_device_records)
                logger.info(f"Stored {devices_stored} Intune devices for {tenant_name}")

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
