from datetime import datetime
import logging
from typing import Any

import pytz

from db.db_client import init_schema, upsert_many
from shared.amx_api import AutomoxApi, AutomoxError, format_datetime
from shared.utils import clean_error_message


logger = logging.getLogger(__name__)


def transform_organization_data(org_data: dict[str, Any]) -> dict[str, Any]:
    """
    Transform raw organization data from Automox API into database format.

    Args:
        org_data: Raw organization data from Automox API

    Returns:
        Transformed organization data for database storage
    """
    # Extract name string - format is "DisplayName:ConnectwiseID"
    name_str = org_data.get("name", "Unknown:0")

    if ":" in name_str:
        display_name, connectwise_id_str = name_str.split(":", 1)
        try:
            connectwise_id = int(connectwise_id_str)
        except (ValueError, TypeError):
            connectwise_id = 0
    else:
        display_name = name_str
        connectwise_id = 0

    return {
        "organization_id": org_data.get("id"),
        "connectwise_id": connectwise_id,
        "display_name": display_name,
        "device_count": org_data.get("device_count"),
        "created_at": datetime.now(pytz.UTC).isoformat(),
        "last_updated": datetime.now(pytz.UTC).isoformat(),
    }


def sync_automox_organizations() -> dict[str, Any]:
    """
    Sync Automox organizations data to database.

    Returns:
        Dictionary containing sync results and statistics
    """
    logger.info("Starting Automox organizations sync")
    start_time = datetime.now(pytz.UTC)

    try:
        # Initialize database schema
        init_schema()

        # Get organizations from Automox API
        with AutomoxApi() as api:
            logger.info("Fetching organizations from Automox API")
            orgs_data = api.get_all_organizations()

            if not orgs_data:
                logger.warning("No organizations found in Automox API response")
                return {
                    "status": "success",
                    "organizations_synced": 0,
                    "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
                    "message": "No organizations found to sync",
                }

            logger.info(f"Found {len(orgs_data)} organizations in Automox API")

            # Transform data for database
            transformed_orgs = []
            for org in orgs_data:
                try:
                    transformed_org = transform_organization_data(org)
                    transformed_orgs.append(transformed_org)
                except Exception as e:
                    logger.error(f"Error transforming organization data: {e}")
                    continue

            if not transformed_orgs:
                logger.error("No valid organizations to sync after transformation")
                return {
                    "status": "error",
                    "error": "No valid organizations to sync after transformation",
                    "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
                }

            # Insert/update organizations in database
            logger.info(f"Syncing {len(transformed_orgs)} organizations to database")
            upsert_many("amx_orgs", transformed_orgs)

            duration = (datetime.now(pytz.UTC) - start_time).total_seconds()
            logger.info(f"Successfully synced {len(transformed_orgs)} organizations in {duration:.2f}s")

            return {
                "status": "success",
                "organizations_synced": len(transformed_orgs),
                "duration_seconds": duration,
                "message": f"Successfully synced {len(transformed_orgs)} organizations",
            }

    except AutomoxError as e:
        error_msg = f"Automox API error: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {"status": "error", "error": error_msg, "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds()}

    except Exception as e:
        error_msg = f"Unexpected error during Automox organizations sync: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {"status": "error", "error": error_msg, "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds()}


def get_organization_statistics() -> dict[str, Any]:
    """
    Get statistics about synced organizations.

    Returns:
        Dictionary containing organization statistics
    """
    try:
        from db.db_client import get_connection

        conn = get_connection()
        cursor = conn.cursor()

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM amx_orgs")
        total_orgs = cursor.fetchone()[0]

        # Get organizations with devices
        cursor.execute("SELECT COUNT(*) FROM amx_orgs WHERE device_count > 0")
        orgs_with_devices = cursor.fetchone()[0]

        # Get total device count
        cursor.execute("SELECT SUM(device_count) FROM amx_orgs WHERE device_count IS NOT NULL")
        total_devices = cursor.fetchone()[0] or 0

        # Get latest sync time
        cursor.execute("SELECT MAX(last_updated) FROM amx_orgs")
        latest_sync = cursor.fetchone()[0]

        conn.close()

        return {
            "total_organizations": total_orgs,
            "organizations_with_devices": orgs_with_devices,
            "total_devices": total_devices,
            "latest_sync": latest_sync,
        }

    except Exception as e:
        logger.error(f"Error getting organization statistics: {e}")
        return {"total_organizations": 0, "organizations_with_devices": 0, "total_devices": 0, "latest_sync": None, "error": str(e)}


def transform_device_data(device_data: dict[str, Any], org_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Transform raw device data from Automox API into database format for both tables.

    Args:
        device_data: Raw device data from Automox API
        org_id: Organization ID this device belongs to

    Returns:
        Tuple of (device_data, device_details_data) for database storage
    """
    import json

    # Handle IP addresses - convert string to list if needed
    ip_addrs = device_data.get("ip_addrs", "")
    if isinstance(ip_addrs, str) and ip_addrs:
        ip_addrs = [ip_addrs]
    elif not ip_addrs:
        ip_addrs = []

    # Basic device data for amx_devices table
    device_data_dict = {
        "organization_id": org_id,
        "device_id": device_data.get("id"),
        "display_name": device_data.get("name"),  # Use 'name' as display_name
        "agent_version": device_data.get("agent_version"),
        "server_group_id": device_data.get("server_group_id"),  # Now available with include_details=1
        "connected": device_data.get("is_connected"),  # Use 'is_connected'
        "is_compliant": device_data.get("is_compliant"),
        "pending_patches": device_data.get("pending_patches"),
        "needs_reboot": device_data.get("needs_reboot"),
        "next_patch_time": format_datetime(device_data.get("next_patch_time")),  # Now available with include_next_patch_time=1
        "inventory_last_refresh_time": format_datetime(device_data.get("last_refresh_time")),
        "ip_addrs": json.dumps(ip_addrs),
        "ip_addrs_private": json.dumps(device_data.get("ip_addrs_private", [])),  # Now available with include_details=1
        "created_at": datetime.now(pytz.UTC).isoformat(),
        "last_updated": datetime.now(pytz.UTC).isoformat(),
    }

    # Detailed device data for amx_device_details table
    device_details_dict = {
        "organization_id": org_id,
        "device_id": device_data.get("id"),
        "os_family": device_data.get("os_family"),
        "os_name": device_data.get("os_name"),  # Now available with include_details=1
        "os_version": device_data.get("os_version"),
        "os_version_id": device_data.get("os_version_id"),  # Now available with include_details=1
        "serial_number": device_data.get("serial_number"),
        "model": device_data.get("model"),
        "vendor": device_data.get("vendor"),
        "version": device_data.get("version"),
        "mdm_server": device_data.get("mdm_server"),
        "mdm_profile_installed": device_data.get("mdm_profile_installed"),
        "secure_token_account": device_data.get("secure_token_account"),
        "last_logged_in_user": device_data.get("last_logged_in_user"),
        "last_process_time": format_datetime(device_data.get("last_process_time")),
        "last_disconnect_time": format_datetime(device_data.get("last_disconnect_time")),
        "is_delayed_by_user": device_data.get("is_delayed_by_user"),
        "needs_attention": device_data.get("needs_attention"),
        "is_compatible": device_data.get("is_compatible"),
        "create_time": format_datetime(device_data.get("create_time")),
        "created_at": datetime.now(pytz.UTC).isoformat(),
        "last_updated": datetime.now(pytz.UTC).isoformat(),
    }

    return device_data_dict, device_details_dict


def sync_automox_devices() -> dict[str, Any]:
    """
    Sync Automox devices data to database for all organizations.

    Returns:
        Dictionary containing sync results and statistics
    """
    logger.info("Starting Automox devices sync")
    start_time = datetime.now(pytz.UTC)

    try:
        # Initialize database schema
        init_schema()

        # Get all organizations first
        with AutomoxApi() as api:
            logger.info("Fetching organizations from Automox API")
            orgs_data = api.get_all_organizations()

            if not orgs_data:
                logger.warning("No organizations found in Automox API response")
                return {
                    "status": "success",
                    "devices_synced": 0,
                    "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
                    "message": "No organizations found to sync devices for",
                }

            logger.info(f"Found {len(orgs_data)} organizations, syncing devices for each")

            total_devices = 0
            org_results = []

            for org in orgs_data:
                org_id = org.get("id")
                org_name = org.get("name", "Unknown")

                try:
                    logger.info(f"Syncing devices for organization {org_name} (ID: {org_id})")

                    # Get devices for this organization
                    devices_data = api.get_all_device_details_by_organization(org_id)

                    if not devices_data:
                        logger.info(f"No devices found for organization {org_name}")
                        continue

                    # Transform data for database
                    transformed_devices = []
                    transformed_device_details = []
                    for device in devices_data:
                        try:
                            device_dict, device_details_dict = transform_device_data(device, org_id)
                            transformed_devices.append(device_dict)
                            transformed_device_details.append(device_details_dict)
                        except Exception as e:
                            logger.error(f"Error transforming device data: {e}")
                            continue

                    if transformed_devices:
                        # Insert/update both devices and device details in database
                        upsert_many("amx_devices", transformed_devices)
                        upsert_many("amx_device_details", transformed_device_details)
                        device_count = len(transformed_devices)
                        total_devices += device_count
                        org_results.append({"org_id": org_id, "org_name": org_name, "devices_synced": device_count})
                        logger.info(f"Synced {device_count} devices and details for {org_name}")

                except AutomoxError as e:
                    # Handle 403 errors gracefully (permission denied)
                    if e.status_code == 403:
                        logger.warning(f"Access denied for organization {org_name} (ID: {org_id}): {e}")
                        org_results.append({"org_id": org_id, "org_name": org_name, "devices_synced": 0, "error": "Access denied (403)"})
                    else:
                        logger.error(f"Automox API error syncing devices for organization {org_name}: {e}")
                        org_results.append({"org_id": org_id, "org_name": org_name, "devices_synced": 0, "error": str(e)})
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error syncing devices for organization {org_name}: {e}")
                    org_results.append({"org_id": org_id, "org_name": org_name, "devices_synced": 0, "error": str(e)})
                    continue

            duration = (datetime.now(pytz.UTC) - start_time).total_seconds()
            logger.info(f"Successfully synced {total_devices} devices across {len(orgs_data)} organizations in {duration:.2f}s")

            return {
                "status": "success",
                "devices_synced": total_devices,
                "organizations_processed": len(orgs_data),
                "organization_results": org_results,
                "duration_seconds": duration,
                "message": f"Successfully synced {total_devices} devices across {len(orgs_data)} organizations",
            }

    except AutomoxError as e:
        error_msg = f"Automox API error: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {"status": "error", "error": error_msg, "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds()}

    except Exception as e:
        error_msg = f"Unexpected error during Automox devices sync: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {"status": "error", "error": error_msg, "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds()}


def get_device_statistics() -> dict[str, Any]:
    """
    Get statistics about synced devices.

    Returns:
        Dictionary containing device statistics
    """
    try:
        from db.db_client import get_connection

        conn = get_connection()
        cursor = conn.cursor()

        # Get total device count
        cursor.execute("SELECT COUNT(*) FROM amx_devices")
        total_devices = cursor.fetchone()[0]

        # Get connected devices
        cursor.execute("SELECT COUNT(*) FROM amx_devices WHERE connected = 1")
        connected_devices = cursor.fetchone()[0]

        # Get compliant devices
        cursor.execute("SELECT COUNT(*) FROM amx_devices WHERE is_compliant = 1")
        compliant_devices = cursor.fetchone()[0]

        # Get devices by OS family
        cursor.execute("SELECT os_family, COUNT(*) FROM amx_devices WHERE os_family IS NOT NULL GROUP BY os_family")
        os_families = dict(cursor.fetchall())

        # Get devices needing reboot
        cursor.execute("SELECT COUNT(*) FROM amx_devices WHERE needs_reboot = 1")
        needs_reboot = cursor.fetchone()[0]

        # Get total pending patches
        cursor.execute("SELECT SUM(pending_patches) FROM amx_devices WHERE pending_patches IS NOT NULL")
        total_pending_patches = cursor.fetchone()[0] or 0

        # Get latest sync time
        cursor.execute("SELECT MAX(last_updated) FROM amx_devices")
        latest_sync = cursor.fetchone()[0]

        conn.close()

        return {
            "total_devices": total_devices,
            "connected_devices": connected_devices,
            "compliant_devices": compliant_devices,
            "needs_reboot": needs_reboot,
            "total_pending_patches": total_pending_patches,
            "os_families": os_families,
            "latest_sync": latest_sync,
        }

    except Exception as e:
        logger.error(f"Error getting device statistics: {e}")
        return {
            "total_devices": 0,
            "connected_devices": 0,
            "compliant_devices": 0,
            "needs_reboot": 0,
            "total_pending_patches": 0,
            "os_families": {},
            "latest_sync": None,
            "error": str(e),
        }


def transform_package_data(package_data: dict[str, Any], org_id: int, device_id: int) -> dict[str, Any]:
    """
    Transform raw package data from Automox API into database format.

    Args:
        package_data: Raw package data from Automox API
        org_id: Organization ID this package belongs to
        device_id: Device ID this package belongs to

    Returns:
        Transformed package data for database storage
    """
    import json

    # Handle CVE list - convert to JSON string if it's a list
    cves = package_data.get("cves", [])
    if isinstance(cves, list):
        cves_json = json.dumps(cves)
    else:
        cves_json = json.dumps([]) if not cves else str(cves)

    return {
        "organization_id": org_id,
        "device_id": device_id,
        "package_id": package_data.get("package_id"),
        "software_id": package_data.get("software_id"),
        "display_name": package_data.get("display_name", "Unknown Package"),
        "name": package_data.get("name"),
        "package_version_id": package_data.get("package_version_id"),
        "version": package_data.get("version"),
        "repo": package_data.get("repo"),
        "installed": package_data.get("installed"),
        "ignored": package_data.get("ignored"),
        "group_ignored": package_data.get("group_ignored"),
        "deferred_until": format_datetime(package_data.get("deferred_until")),
        "group_deferred_until": format_datetime(package_data.get("group_deferred_until")),
        "requires_reboot": package_data.get("requires_reboot"),
        "severity": package_data.get("severity"),
        "cve_score": package_data.get("cve_score"),
        "cves": cves_json,
        "is_managed": package_data.get("is_managed"),
        "impact": package_data.get("impact"),
        "os_name": package_data.get("os_name"),
        "os_version": package_data.get("os_version"),
        "scheduled_at": format_datetime(package_data.get("patch_time")),
        "created_at": datetime.now(pytz.UTC).isoformat(),
        "last_updated": datetime.now(pytz.UTC).isoformat(),
    }


def sync_automox_packages() -> dict[str, Any]:
    """
    Sync Automox packages data to database for all organizations.

    Returns:
        Dictionary containing sync results and statistics
    """
    logger.info("Starting Automox packages sync")
    start_time = datetime.now(pytz.UTC)

    try:
        # Initialize database schema
        init_schema()

        # Get all organizations first
        with AutomoxApi() as api:
            logger.info("Fetching organizations from Automox API")
            orgs_data = api.get_all_organizations()

            if not orgs_data:
                logger.warning("No organizations found in Automox API response")
                return {
                    "status": "success",
                    "packages_synced": 0,
                    "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
                    "message": "No organizations found to sync packages for",
                }

            logger.info(f"Found {len(orgs_data)} organizations, syncing packages for each")

            total_packages = 0
            org_results = []

            for org in orgs_data:
                org_id = org.get("id")
                org_name = org.get("name", "Unknown")

                try:
                    logger.info(f"Syncing packages for organization {org_name} (ID: {org_id})")

                    # Get packages for this organization
                    packages_data = api.get_packages_by_organization(org_id, org_name)

                    if not packages_data:
                        logger.info(f"No packages found for organization {org_name}")
                        continue

                    # Transform data for database
                    transformed_packages = []
                    for package in packages_data:
                        try:
                            # Extract device_id from package data
                            device_id = package.get("server_id")
                            if not device_id:
                                logger.warning(f"Package missing server_id, skipping: {package.get('display_name', 'Unknown')}")
                                continue

                            transformed_package = transform_package_data(package, org_id, device_id)
                            transformed_packages.append(transformed_package)
                        except Exception as e:
                            logger.error(f"Error transforming package data: {e}")
                            continue

                    if transformed_packages:
                        # Insert/update packages in database
                        upsert_many("amx_packages", transformed_packages)
                        package_count = len(transformed_packages)
                        total_packages += package_count
                        org_results.append({"org_id": org_id, "org_name": org_name, "packages_synced": package_count})
                        logger.info(f"Synced {package_count} packages for {org_name}")

                except AutomoxError as e:
                    # Handle 403 errors gracefully (permission denied)
                    if e.status_code == 403:
                        logger.warning(f"Access denied for organization {org_name} (ID: {org_id}): {e}")
                        org_results.append({"org_id": org_id, "org_name": org_name, "packages_synced": 0, "error": "Access denied (403)"})
                    else:
                        logger.error(f"Automox API error syncing packages for organization {org_name}: {e}")
                        org_results.append({"org_id": org_id, "org_name": org_name, "packages_synced": 0, "error": str(e)})
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error syncing packages for organization {org_name}: {e}")
                    org_results.append({"org_id": org_id, "org_name": org_name, "packages_synced": 0, "error": str(e)})
                    continue

            duration = (datetime.now(pytz.UTC) - start_time).total_seconds()
            logger.info(f"Successfully synced {total_packages} packages across {len(orgs_data)} organizations in {duration:.2f}s")

            return {
                "status": "success",
                "packages_synced": total_packages,
                "organizations_processed": len(orgs_data),
                "organization_results": org_results,
                "duration_seconds": duration,
                "message": f"Successfully synced {total_packages} packages across {len(orgs_data)} organizations",
            }

    except AutomoxError as e:
        error_msg = f"Automox API error: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {
            "status": "error",
            "packages_synced": 0,
            "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
            "message": error_msg,
        }
    except Exception as e:
        error_msg = f"Unexpected error: {clean_error_message(str(e))}"
        logger.error(error_msg)
        return {
            "status": "error",
            "packages_synced": 0,
            "duration_seconds": (datetime.now(pytz.UTC) - start_time).total_seconds(),
            "message": error_msg,
        }


def get_package_statistics() -> dict[str, Any]:
    """
    Get statistics from the amx_packages table.

    Returns:
        Dictionary containing package statistics
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Get total packages count
        cursor.execute("SELECT COUNT(*) FROM amx_packages")
        total_packages = cursor.fetchone()[0]

        # Get installed packages count
        cursor.execute("SELECT COUNT(*) FROM amx_packages WHERE installed = 1")
        installed_packages = cursor.fetchone()[0]

        # Get packages by severity
        cursor.execute("""
            SELECT severity, COUNT(*) as count 
            FROM amx_packages 
            WHERE severity IS NOT NULL 
            GROUP BY severity 
            ORDER BY count DESC
        """)
        severity_counts = dict(cursor.fetchall())

        # Get packages by repository
        cursor.execute("""
            SELECT repo, COUNT(*) as count 
            FROM amx_packages 
            WHERE repo IS NOT NULL 
            GROUP BY repo 
            ORDER BY count DESC
        """)
        repo_counts = dict(cursor.fetchall())

        # Get packages requiring reboot
        cursor.execute("SELECT COUNT(*) FROM amx_packages WHERE requires_reboot = 1")
        requires_reboot = cursor.fetchone()[0]

        # Get packages with CVEs
        cursor.execute("SELECT COUNT(*) FROM amx_packages WHERE cve_score IS NOT NULL AND cve_score > 0")
        packages_with_cves = cursor.fetchone()[0]

        # Get latest sync time
        cursor.execute("SELECT MAX(last_updated) FROM amx_packages")
        latest_sync = cursor.fetchone()[0]

        conn.close()

        return {
            "total_packages": total_packages,
            "installed_packages": installed_packages,
            "requires_reboot": requires_reboot,
            "packages_with_cves": packages_with_cves,
            "severity_breakdown": severity_counts,
            "repository_breakdown": repo_counts,
            "latest_sync": latest_sync,
        }

    except Exception as e:
        logger.error(f"Error getting package statistics: {e}")
        return {
            "total_packages": 0,
            "installed_packages": 0,
            "requires_reboot": 0,
            "packages_with_cves": 0,
            "severity_breakdown": {},
            "repository_breakdown": {},
            "latest_sync": None,
            "error": str(e),
        }
