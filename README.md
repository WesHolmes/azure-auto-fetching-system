# Azure Functions - Graph API Sync

This Azure Functions project provides automated synchronization of Microsoft Graph API data to a local SQLite database. It supports both single-tenant and multi-tenant scenarios.

## Features

- **Timer-triggered sync**: Automated hourly user sync and daily service principal sync
- **HTTP endpoints**: Manual sync triggers for on-demand synchronization
- **Tenant flexibility**: Single tenant or multi-tenant mode via configuration
- **Service principal auth**: Uses client credentials flow for unattended operation
- **SQLite storage**: Lightweight database with tenant isolation
- **Pagination support**: Handles large datasets with Microsoft Graph pagination
- **Rate limiting**: Built-in retry logic with exponential backoff

## Project Structure

```
azure-functions/
├── function_app.py                    # Function triggers and entry points
├── requirements.txt                   # Python dependencies
├── local.settings.json               # Local configuration (not in source control)
├── host.json                         # Azure Functions runtime configuration
├── core/                             # Core utilities
│   ├── __init__.py
│   ├── graph_client.py               # Microsoft Graph API client
│   ├── database.py                   # SQLite database operations
│   └── tenant_manager.py             # Tenant resolution logic
├── sync/                             # Sync business logic
│   ├── __init__.py
│   ├── user_sync.py                  # User synchronization
│   └── service_principal_sync.py     # Service principal synchronization
└── data/                             # Database storage (created at runtime)
    └── graph_sync.db                 # SQLite database file
```

## Setup Instructions

### Prerequisites

- Python 3.12
- Azure Functions Core Tools
- Azure AD application with appropriate Graph API permissions

### 1. Clone and Install

```bash
cd azure-functions
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file for local development:

```bash
cp .env.sample .env
# Edit .env with your actual values
```

Required environment variables:
- `CLIENT_ID` - Azure AD application client ID
- `CLIENT_SECRET` - Azure AD application client secret
- `PARTNER_TENANT_ID` - (Optional) For multi-tenant mode

Or create/update `local.settings.json`:

#### Single Tenant Mode

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "TENANT_MODE": "single",
    "TENANT_ID": "your-tenant-id",
    "TENANT_NAME": "Your Organization",
    "AZURE_CLIENT_ID": "your-app-client-id",
    "CLIENT_SECRET": "your-app-client-secret",
    "DATABASE_PATH": "./data/graph_sync.db"
  }
}
```

#### Multi-Tenant Mode

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "TENANT_MODE": "multi",
    "PARTNER_TENANT_ID": "your-partner-tenant-id",
    "AZURE_CLIENT_ID": "your-multi-tenant-app-id",
    "CLIENT_SECRET": "your-multi-tenant-app-secret",
    "DATABASE_PATH": "./data/graph_sync.db"
  }
}
```

### 3. Required Graph API Permissions

Configure your Azure AD application with these permissions:
- `User.Read.All` - Read all users' profiles
- `Application.Read.All` - Read all applications and service principals
- `Customer.Read.All` - Read customer tenant information (multi-tenant only)

### 4. Run Locally

```bash
func start
```

The functions will be available at:
- Timer triggers run automatically based on schedule
- HTTP endpoints available at `http://localhost:7071/api/`

### 5. Test Manual Sync

```bash
# Trigger user sync
curl -X POST http://localhost:7071/api/sync/users

# Trigger service principal sync
curl -X POST http://localhost:7071/api/sync/serviceprincipals
```

## Database Schema

### Users Table (Legacy)

| Column | Type | Description |
|--------|------|-------------|
| id | TEXT | User object ID from Azure AD |
| tenant_id | TEXT | Tenant identifier |
| display_name | TEXT | User's display name |
| user_principal_name | TEXT | User's UPN (email) |
| mail | TEXT | User's email address |
| account_enabled | BOOLEAN | Account status |
| user_type | TEXT | Member or Guest |
| department | TEXT | User's department |
| job_title | TEXT | User's job title |
| office_location | TEXT | User's office location |
| mobile_phone | TEXT | User's mobile phone number |
| last_sign_in | TEXT | Last sign-in timestamp |
| synced_at | TEXT | Last sync timestamp |

Primary Key: (id, tenant_id)

### UsersV2 Table (Current)

| Column | Type | Description |
|--------|------|-------------|
| user_id | TEXT(50) | User object ID from Azure AD |
| tenant_id | TEXT(50) | Tenant identifier |
| user_principal_name | TEXT(255) | User's UPN (email) |
| primary_email | TEXT(255) | User's primary email address |
| display_name | TEXT(255) | User's display name |
| department | TEXT(100) | User's department |
| job_title | TEXT(100) | User's job title |
| office_location | TEXT(100) | User's office location |
| mobile_phone | TEXT(50) | User's mobile phone number |
| account_type | TEXT(50) | Member or Guest |
| account_enabled | INTEGER | Account status (1=enabled, 0=disabled) |
| is_global_admin | INTEGER | Global admin status (1=yes, 0=no) |
| is_mfa_compliant | INTEGER | MFA compliance (1=compliant, 0=not compliant) |
| license_count | INTEGER | Number of assigned licenses |
| group_count | INTEGER | Number of group memberships |
| last_sign_in_date | TEXT | Last sign-in timestamp (ISO format) |
| last_password_change | TEXT | Last password change timestamp (ISO format) |
| created_at | TEXT | Record creation timestamp |
| last_updated | TEXT | Last update timestamp |

Primary Key: (user_id, tenant_id)

### Service Principals Table

| Column | Type | Description |
|--------|------|-------------|
| id | TEXT | Service principal object ID |
| tenant_id | TEXT | Tenant identifier |
| app_id | TEXT | Application ID |
| display_name | TEXT | Service principal name |
| publisher_name | TEXT | Application publisher |
| service_principal_type | TEXT | Type of service principal |
| synced_at | TEXT | Last sync timestamp |

Primary Key: (id, tenant_id)

## Function Schedule

- **User Sync**: Runs hourly (0 */1 * * * *)
- **Service Principal Sync**: Runs daily at 2 AM (0 0 2 * * *)

## Deployment to Azure

### 1. Create Function App

```bash
az functionapp create \
  --resource-group myResourceGroup \
  --consumption-plan-location westus \
  --runtime python \
  --runtime-version 3.9 \
  --functions-version 4 \
  --name myFunctionApp \
  --storage-account mystorageaccount
```

### 2. Configure Application Settings

```bash
az functionapp config appsettings set \
  --name myFunctionApp \
  --resource-group myResourceGroup \
  --settings \
    TENANT_MODE=single \
    TENANT_ID=your-tenant-id \
    TENANT_NAME="Your Organization" \
    AZURE_CLIENT_ID=your-client-id \
    CLIENT_SECRET=@Microsoft.KeyVault(SecretUri=https://myvault.vault.azure.net/secrets/client-secret/)
```

### 3. Deploy

```bash
func azure functionapp publish myFunctionApp
```

## Security Best Practices

1. **Use Key Vault**: Store CLIENT_SECRET in Azure Key Vault
2. **Enable Managed Identity**: Use managed identity where possible
3. **Network Restrictions**: Configure IP restrictions and VNET integration
4. **Authentication**: Enable function-level authentication for HTTP endpoints
5. **Least Privilege**: Grant only required Graph API permissions

## Monitoring

- **Application Insights**: Enable for detailed telemetry
- **Function Logs**: View in Azure Portal or via CLI
- **Metrics**: Monitor execution count, duration, and failures
- **Alerts**: Set up alerts for sync failures

## Troubleshooting

### Common Issues

1. **"Insufficient privileges" error**
   - Ensure admin consent is granted for Graph API permissions
   - Verify the service principal has required permissions

2. **"Invalid tenant" error**
   - Check TENANT_ID is correct
   - For multi-tenant, ensure app is configured for multi-tenancy

3. **Database locked errors**
   - Ensure only one instance is running locally
   - Check file permissions on data directory

4. **No data syncing**
   - Verify Graph API returns data using Graph Explorer
   - Check function logs for specific error messages
   - Ensure DATABASE_PATH directory exists and is writable

5. **Rate limiting (429) errors**
   - Built-in retry logic should handle most cases
   - For persistent issues, reduce batch sizes or increase delays

### Debug Mode

Enable detailed logging by setting:
```json
{
  "logging": {
    "logLevel": {
      "default": "Debug"
    }
  }
}
```

## Development

### Adding New Entity Types

1. Update database schema in `core/database.py`
2. Create new sync module in `sync/` directory
3. Add function trigger in `function_app.py`

Example:
```python
# sync/group_sync.py
def sync_groups(tenant_id: str, tenant_name: str):
    client = GraphClient(tenant_id)
    groups = client.get_all('/groups', select=['id', 'displayName', 'description'])
    # Transform and save to database
```

### Python Linter

Run Ruff Linter:
```bash
ruff format <<python.py>>
```

## Support

For issues or questions:
1. Check function logs in Azure Portal
2. Review Application Insights telemetry
3. Enable debug logging for detailed traces