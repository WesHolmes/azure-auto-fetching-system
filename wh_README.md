# Azure Functions Implementation Guide

This document covers the working implementation of the Azure Functions app for Azure AD synchronization and analysis.

## Overview

The Azure Functions app provides automated synchronization of Azure AD data (applications, devices, policies) from the Microsoft Graph API and exposes HTTP endpoints for data retrieval from our SQLite database and analysis. It uses a V2 database schema for improved data storage and retrieval.

## Core Components

### Relevant Database Schema (V2)

The app uses SQLite with the following main tables:
- `applications_v2` - Service principals and applications
- `devices_v2` - Managed devices and Azure AD registered devices  
- `policies_v2` - Conditional access policies
- `user_policies_v2` - User-policy assignments
- `application_policies_v2` - Application-policy assignments

### Relevant Synchronization Functions

#### Applications Sync (`applications_sync_v2.py`)
- Fetches service principals from Microsoft Graph API
- Retrieves owner information and sign-in activity
- Stores data in `applications_v2` table
- Runs daily at 5:00 AM via timer trigger

#### Devices Sync (`device_sync_v2.py`)
- Fetches Intune managed devices and Azure AD registered devices
- Handles missing Intune licensing gracefully
- Combines device data into unified `devices_v2` table
- Runs every 10 minutes via timer trigger

#### Policies Sync (`policies_sync_v2.py`)
- Fetches conditional access policies from Microsoft Graph
- Resolves user and application assignments based on policy conditions
- Creates mapping tables for policy relationships
- Runs daily at 12:20 AM via timer trigger

## HTTP Endpoints

### Data Retrieval Endpoints

#### GET `/api/tenant/serviceprincipals`
Retrieves all service principals from local database. This is a list of all Service Principals in the database_v2. Making the information consumable to a front end.

**Response:**
```json
{
  "success": true,
  "data": [...],
  "metadata": {
    "tenant_id": "...",
    "total_applications": 150,
    "enabled_count": 120,
    "disabled_count": 30
  }
}
```

#### GET `/api/tenant/policies`
Retrieves all conditional access policies from local database. This is a list of all Policies in the database_v2. Making the information consumable to a front end.

**Response:**
```json
{
  "success": true,
  "data": [...],
  "metadata": {
    "tenant_id": "...",
    "total_policies": 25,
    "active_count": 20,
    "inactive_count": 5
  }
}
```

#### GET `/api/tenant/devices`
Retrieves all devices (managed and Azure AD registered) from local database. This is a list of all Devices in the database_v2. Making the information consumable to a front end.

**Response:**
```json
{
  "success": true,
  "data": [...],
  "metadata": {
    "tenant_id": "...",
    "total_devices": 500,
    "managed_count": 450,
    "unmanaged_count": 50
  }
}
```

### Analysis Endpoint

#### GET `/api/tenant/analysis`
Provides comprehensive service principal analysis and actionable insights.

{
    "total_applications": 150,
    "enabled_count": 120,
    "disabled_count": 30
}

**What it does:**
1. Analyzes all service principals in the local database
2. Identifies inactive service principals (no sign-in within 90 days)
3. Detects expired and expiring credentials
4. Calculates risk levels based on inactive accounts
5. Generates prioritized action recommendations

**Response:**
```json
{
  "success": true,
  "metadata": {
    "total_service_principals": 150,
    "inactive_service_principals": 25,
    "expired_credentials": 5,
    "expiring_credentials": 3,
    "risk_level": 16.7
  },
  "actions": [
    {
      "title": "Review Inactive Service Principals",
      "description": "25 service principals with no sign-in activity within past 90 days",
      "action": "review_inactive",
      "priority": "high"
    }
  ]
}
```

## Manual Sync Endpoints

### POST `/api/sync/applications-v2`
Manually triggers applications synchronization for the configured tenant.

### POST `/api/sync/policies-v2`
Manually triggers policies synchronization for the configured tenant.

### POST `/api/sync/devices-v2`
Manually triggers devices synchronization for the configured tenant.

## How to Use

### 1. Data Retrieval
```bash
# Get all service principals
curl -X GET "https://your-function-app.azurewebsites.net/api/tenant/serviceprincipals"

# Get all policies
curl -X GET "https://your-function-app.azurewebsites.net/api/tenant/policies"

# Get all devices
curl -X GET "https://your-function-app.azurewebsites.net/api/tenant/devices"
```

### 2. Analysis
```bash
# Get service principal analysis and recommendations
curl -X GET "https://your-function-app.azurewebsites.net/api/tenant/analysis"
```

### 3. Manual Sync
```bash
# Manually sync applications
curl -X POST "https://your-function-app.azurewebsites.net/api/sync/applications-v2"

# Manually sync policies
curl -X POST "https://your-function-app.azurewebsites.net/api/sync/policies-v2"

# Manually sync devices
curl -X POST "https://your-function-app.azurewebsites.net/api/sync/devices-v2"
```

## What the Azure Functions Do

### Automated Synchronization
- **Timer Triggers**: Run on schedule to keep data fresh
- **Error Handling**: Gracefully handles API failures and missing permissions
- **Data Transformation**: Converts Graph API responses to database format
- **Incremental Updates**: Uses upsert operations to avoid duplicates

### Data Analysis
- **Risk Assessment**: Identifies security and compliance issues
- **Actionable Insights**: Provides specific recommendations for administrators
- **Performance Metrics**: Tracks sync performance and success rates

### API Layer
- **RESTful Endpoints**: Standard HTTP methods for data access
- **Structured Responses**: Consistent JSON format with metadata
- **Error Handling**: Proper HTTP status codes and error messages
- **Single Tenant Mode**: Automatically uses configured tenant

## Database Operations

The app uses `upsert_many_v2()` function for efficient database operations:
- Inserts new records
- Updates existing records based on primary keys
- Handles large datasets efficiently
- Maintains data consistency

## Error Handling

- **API Failures**: Logs errors and continues processing other tenants
- **Missing Permissions**: Gracefully handles insufficient Graph API permissions
- **Database Errors**: Logs database operation failures
- **Rate Limiting**: Implements concurrent request limits to avoid API throttling

## Monitoring

- **Logging**: Comprehensive logging for all operations
- **Metrics**: Tracks sync performance and success rates
- **Error Reporting**: Centralized error categorization and reporting
- **Health Checks**: Endpoint availability and data freshness indicators
