# Shared infrastructure components

# Make shared imports available
from .error_reporting import aggregate_recent_sync_errors, categorize_sync_errors
from .graph_beta_client import GraphBetaClient
from .graph_client import GraphClient, get_tenants
from .utils import clean_error_message, create_bulk_operation_response, create_error_response, create_success_response


__all__ = [
    "GraphClient",
    "GraphBetaClient",
    "get_tenants",
    "clean_error_message",
    "create_error_response",
    "create_success_response",
    "create_bulk_operation_response",
    "categorize_sync_errors",
    "aggregate_recent_sync_errors",
]
