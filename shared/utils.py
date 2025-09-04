from datetime import datetime
import json
from typing import Any

import azure.functions as func


def clean_error_message(error_str: str, context: str = "", tenant_name: str = "") -> str:
    """
    Clean up error messages for better console readability.

    Args:
        error_str: The original error string
        context: Context about what operation failed (e.g., "Failed to fetch groups")
        tenant_name: Optional tenant name to include in the error

    Returns:
        Clean, readable error message
    """
    # Common HTTP error patterns
    if "401 Unauthorized" in error_str:
        if tenant_name:
            return f"✗ {tenant_name}: Authentication failed (401 Unauthorized)"
        elif context:
            return f"✗ {context}: Authentication failed (401 Unauthorized)"
        else:
            return "✗ Authentication failed (401 Unauthorized)"

    elif "403 Forbidden" in error_str:
        if tenant_name:
            return f"✗ {tenant_name}: Access denied (403 Forbidden)"
        elif context:
            return f"✗ {context}: Access denied (403 Forbidden)"
        else:
            return "✗ Access denied (403 Forbidden)"

    elif "404 Not Found" in error_str:
        if tenant_name:
            return f"✗ {tenant_name}: Resource not found (404)"
        elif context:
            return f"✗ {context}: Resource not found (404)"
        else:
            return "✗ Resource not found (404)"

    elif "500 Internal Server Error" in error_str:
        if tenant_name:
            return f"✗ {tenant_name}: Server error (500)"
        elif context:
            return f"✗ {context}: Server error (500)"
        else:
            return "✗ Server error (500)"

    else:
        # For other errors, include context if available
        if tenant_name:
            return f"✗ {tenant_name}: {error_str}"
        elif context:
            return f"✗ {context}: {error_str}"
        else:
            return f"✗ {error_str}"


def create_metadata(tenant_id: str, tenant_name: str, operation: str, **additional_fields) -> dict[str, Any]:
    metadata = {
        "tenant_id": tenant_id,
        "tenant_name": tenant_name,
        "operation": operation,
        "timestamp": datetime.now().isoformat(),
    }

    # Add any additional fields
    metadata.update(additional_fields)

    return metadata


def create_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not actions:
        return []

    # Limit to maximum 4 actions for consistency (as seen in your endpoints)
    return actions[:4]


def create_success_response(
    data: list[Any] | dict[str, Any] | Any,
    tenant_id: str,
    tenant_name: str = "unknown",
    operation: str = "unknown",
    actions: list[dict[str, Any]] | None = None,
    metrics: dict[str, Any] | None = None,
    resource_id: str | None = None,
    message: str | None = None,
    **additional_metadata,
) -> func.HttpResponse:
    metadata = create_metadata(tenant_id, tenant_name, operation, **additional_metadata)

    # Add metrics to metadata if provided
    if metrics:
        metadata.update(metrics)

    # Add resource_id to metadata if provided
    if resource_id:
        metadata["resource_id"] = resource_id

    response_data = {
        "success": True,
        "data": data,
        "metadata": metadata,
    }

    # Add message if provided
    if message:
        response_data["message"] = message

    # Add actions if provided
    if actions:
        response_data["actions"] = create_actions(actions)

    return func.HttpResponse(json.dumps(response_data, indent=2), status_code=200, headers={"Content-Type": "application/json"})


def create_error_response(
    error_message: str,
    status_code: int = 500,
    tenant_id: str | None = None,
    tenant_name: str | None = None,
    operation: str | None = None,
    data: list[Any] | dict[str, Any] | Any | None = None,
    actions: list[dict[str, Any]] | None = None,
    **additional_metadata,
) -> func.HttpResponse:
    response_data = {
        "success": False,
        "error": error_message,
    }

    if data is not None:
        response_data["data"] = data

    if tenant_id and tenant_name and operation:
        response_data["metadata"] = create_metadata(tenant_id, tenant_name, operation, **additional_metadata)
    elif additional_metadata:
        response_data["metadata"] = additional_metadata

    if actions:
        response_data["actions"] = create_actions(actions)

    return func.HttpResponse(json.dumps(response_data, indent=2), status_code=status_code, headers={"Content-Type": "application/json"})


def create_bulk_operation_response(
    results: list[dict[str, Any]],
    tenant_id: str,
    operation: str,
    message: str,
    tenant_name: str = "unknown",
    **additional_metadata,
) -> func.HttpResponse:
    metadata = create_metadata(tenant_id, tenant_name, operation, **additional_metadata)

    # Calculate summary from results
    successful = len([r for r in results if r.get("status") == "success"])
    failed = len([r for r in results if r.get("status") == "error"])

    metadata["summary"] = {"total": len(results), "successful": successful, "failed": failed}

    response_data = {
        "success": failed == 0,  # Success if no failures
        "data": results,
        "metadata": metadata,
        "message": message,
    }

    # Determine status code based on results
    if failed == 0 and successful > 0:
        status_code = 200  # Complete success
    elif failed > 0 and successful > 0:
        status_code = 207  # Multi-status - mixed results
    else:
        status_code = 500  # All failed

    return func.HttpResponse(json.dumps(response_data, indent=2), status_code=status_code, headers={"Content-Type": "application/json"})
