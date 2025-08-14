# Azure Functions - Graph API Sync

Automated synchronization of Microsoft Graph API data to a local SQLite database.

## What it does

- Syncs users and service principals from Microsoft Graph API
- Runs on a schedule (hourly for users, daily for service principals)
- Supports both single-tenant and multi-tenant scenarios
- Stores data in SQLite database with tenant isolation

## Quick start

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment**
   - Set `AZURE_CLIENT_ID`, `CLIENT_SECRET`, and `TENANT_ID` in `local.settings.json`
   - Or use environment variables

3. **Run locally**
   ```bash
   func start
   ```

## Project structure

```
├── function_app.py          # Function triggers
├── core/                    # Core utilities (Graph client, database)
├── sync/                    # Sync logic for different entities
├── data/                    # SQLite database storage
└── requirements.txt         # Python dependencies
```

## Required permissions

Your Azure AD app needs:
- `User.Read.All` - Read all users
- `Application.Read.All` - Read all applications

## Functions

- **User Sync**: Hourly timer trigger
- **Service Principal Sync**: Daily timer trigger  
- **Manual Sync**: HTTP endpoints for on-demand sync

## Database

SQLite database with tables for users and service principals, automatically created at runtime.