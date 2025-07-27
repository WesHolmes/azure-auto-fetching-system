import os
import concurrent.futures
from datetime import datetime, timedelta, timezone
from core.graph_client import GraphClient
from core.database import upsert_many
import json
import sqlite3

def fetch_service_principals_optimized(tenant_id, chunk_size=500):
    """Fetch service principals with optimized API calls and chunking"""
    graph = GraphClient(tenant_id)
    
    # Get service principals in chunks to avoid memory issues
    all_service_principals = []
    
    # First, get all service principals with required fields
    service_principals = graph.get('/servicePrincipals', select=[
        'id', 'appId', 'displayName', 'servicePrincipalType', 
        'accountEnabled', 'passwordCredentials', 'keyCredentials'
    ])
    
    print(f"DEBUG: Found {len(service_principals)} service principals for tenant {tenant_id}")
    
    # Process in chunks to avoid overwhelming the API
    for i in range(0, len(service_principals), chunk_size):
        chunk = service_principals[i:i + chunk_size]
        
        # Use parallel requests to get owners for this chunk
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {}
            for sp in chunk:
                sp_id = sp.get('id')
                future = executor.submit(get_sp_owners_safe, graph, sp_id)
                futures[future] = sp
            
            # Collect results
            for future in concurrent.futures.as_completed(futures):
                sp = futures[future]
                try:
                    owners = future.result(timeout=30)  # 30 second timeout per request
                    sp['owners'] = ','.join([
                        owner.get('displayName', owner.get('id', '')) 
                        for owner in owners
                    ]) if owners else None
                except Exception as e:
                    print(f"WARNING: Failed to get owners for SP {sp.get('id')}: {e}")
                    sp['owners'] = None
                
                all_service_principals.append(sp)
    
    return all_service_principals

def get_sp_owners_safe(graph, sp_id):
    """Safely get service principal owners with error handling"""
    try:
        return graph.get(f"/servicePrincipals/{sp_id}/owners")
    except Exception:
        return []

def fetch_last_signin_lookup_optimized(tenant_id):
    """Optimized fetch of last sign-in information"""
    try:
        graph_beta = GraphClient(tenant_id, version="beta")
        signins = graph_beta.get("/reports/servicePrincipalSignInActivities", top=5000)
        return {str(item['appId']): item.get('lastSignInActivity', None) for item in signins if 'appId' in item}
    except Exception as e:
        print(f"WARNING: Failed to fetch sign-in data for tenant {tenant_id}: {e}")
        return {}

def analyze_and_transform_service_principal_records_optimized(service_principals, tenant_id, signin_lookup=None):
    """Optimized analysis and transformation with pre-fetched sign-in data"""
    now = datetime.now(timezone.utc)
    
    # Use provided sign-in lookup or fetch if not provided
    if signin_lookup is None:
        signin_lookup = fetch_last_signin_lookup_optimized(tenant_id)
    
    records = []
    for sp in service_principals:
        # Analyze credentials
        all_creds = sp.get('passwordCredentials', []) + sp.get('keyCredentials', [])
        expired = False
        has_credentials = bool(all_creds)

        # Check for expired credentials
        for cred in all_creds:
            end = cred.get('endDateTime')
            if end:
                try:
                    end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                    if end_dt < now:
                        expired = True
                        break  # Early exit once we find an expired credential
                except Exception:
                    continue

        # Get sign-in data
        app_id = str(sp.get('appId'))
        last_sign_in_activity = signin_lookup.get(app_id)
        if isinstance(last_sign_in_activity, dict):
            last_sign_in = last_sign_in_activity.get('lastSignInDateTime')
        else:
            last_sign_in = None

        record = {
            'id': sp.get('id'),
            'tenant_id': tenant_id,
            'app_id': sp.get('appId'),
            'display_name': sp.get('displayName'),
            'service_principal_type': sp.get('servicePrincipalType'),
            'owners': sp.get('owners'),
            'expired_credentials': expired,
            'has_credentials': has_credentials,
            'enabled_sp': sp.get('accountEnabled', False),
            'last_sign_in': last_sign_in,
            'synced_at': datetime.now().isoformat()
        }
        records.append(record)
    
    return records



def sync_service_principals(tenant_id, tenant_name):
    """Optimized service principal synchronization"""
    try:
        print(f"DEBUG: Starting optimized sync for tenant {tenant_name} ({tenant_id})")
        start_time = datetime.now()
        
        # Parallel fetch of data
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Start both operations in parallel
            sp_future = executor.submit(fetch_service_principals_optimized, tenant_id)
            signin_future = executor.submit(fetch_last_signin_lookup_optimized, tenant_id)
            
            # Get results
            service_principals = sp_future.result()
            signin_lookup = signin_future.result()
        
        print(f"DEBUG: Fetched {len(service_principals)} SPs and sign-in data in {(datetime.now() - start_time).total_seconds():.2f}s")
        
        # Transform data
        transform_start = datetime.now()
        records = analyze_and_transform_service_principal_records_optimized(
            service_principals, tenant_id, signin_lookup
        )
        print(f"DEBUG: Transformed data in {(datetime.now() - transform_start).total_seconds():.2f}s")
        
        # Optimized database storage
        db_start = datetime.now()
        upsert_many('service_principals', records)
        print(f"DEBUG: Database operations completed in {(datetime.now() - db_start).total_seconds():.2f}s")
        
        total_time = (datetime.now() - start_time).total_seconds()
        print(f"DEBUG: Total sync time for {tenant_name}: {total_time:.2f}s")
        
        return {
            'status': 'success',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'service_principals_synced': len(records),
            'sync_time_seconds': total_time
        }

    except Exception as e:
        print(f"ERROR: Service principal sync failed for {tenant_name}: {str(e)}")
        return {
            'status': 'error',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'error': str(e)
        }

def sync_service_principals_parallel(tenants, max_workers=3):
    """Parallel processing of multiple tenants"""
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(sync_service_principals, tenant['tenant_id'], tenant['name']): tenant
            for tenant in tenants
        }
        
        for future in concurrent.futures.as_completed(futures):
            tenant = futures[future]
            try:
                result = future.result()
                results.append(result)
                
                if result['status'] == 'success':
                    print(f"✓ {tenant['name']}: {result['service_principals_synced']} SPs synced in {result.get('sync_time_seconds', 0):.2f}s")
                else:
                    print(f"✗ {tenant['name']}: {result['error']}")
                    
            except Exception as e:
                print(f"✗ {tenant['name']}: {str(e)}")
                results.append({
                    'status': 'error',
                    'tenant_id': tenant['tenant_id'],
                    'tenant_name': tenant['name'],
                    'error': str(e)
                })
    
    return results


# ============================================================================
# ASYNC IMPLEMENTATION FOR PERFORMANCE OPTIMIZATION
# ============================================================================

import asyncio
from core.graph_client import AsyncGraphClient
from core.database import upsert_many_async

async def fetch_service_principals_async(tenant_id):
    """Async fetch of service principals with concurrent owner requests"""
    print(f"DEBUG: Starting async fetch for tenant {tenant_id}")
    
    async with AsyncGraphClient(tenant_id) as graph:
        # Fetch service principals
        service_principals = await graph.get('/servicePrincipals', select=[
            'id', 'appId', 'displayName', 'servicePrincipalType', 
            'accountEnabled', 'passwordCredentials', 'keyCredentials'
        ])
        
        print(f"DEBUG: Found {len(service_principals)} service principals for tenant {tenant_id}")
        
        # Create concurrent requests for owners
        owner_tasks = []
        for sp in service_principals:
            sp_id = sp.get('id')
            task = get_sp_owners_async(graph, sp_id)
            owner_tasks.append(task)
        
        # Execute all owner requests concurrently
        print(f"DEBUG: Fetching owners for {len(owner_tasks)} service principals concurrently")
        owner_results = await asyncio.gather(*owner_tasks, return_exceptions=True)
        
        # Combine results
        for sp, owners_result in zip(service_principals, owner_results):
            if isinstance(owners_result, Exception):
                print(f"WARNING: Failed to get owners for SP {sp.get('id')}: {owners_result}")
                sp['owners'] = None
            else:
                sp['owners'] = ','.join([
                    owner.get('displayName', owner.get('id', '')) 
                    for owner in owners_result
                ]) if owners_result else None
    
    return service_principals

async def get_sp_owners_async(graph, sp_id):
    """Async version of getting service principal owners"""
    try:
        return await graph.get(f"/servicePrincipals/{sp_id}/owners")
    except Exception as e:
        print(f"WARNING: Failed to get owners for SP {sp_id}: {e}")
        return []

async def fetch_last_signin_lookup_async(tenant_id):
    """Async fetch of last sign-in information"""
    try:
        async with AsyncGraphClient(tenant_id, version="beta") as graph_beta:
            signins = await graph_beta.get("/reports/servicePrincipalSignInActivities", top=5000)
            return {str(item['appId']): item.get('lastSignInActivity', None) for item in signins if 'appId' in item}
    except Exception as e:
        print(f"WARNING: Failed to fetch sign-in data for tenant {tenant_id}: {e}")
        return {}

async def sync_service_principals_async(tenant_id, tenant_name):
    """Async service principal synchronization with maximum concurrency"""
    try:
        print(f"DEBUG: Starting async sync for tenant {tenant_name} ({tenant_id})")
        start_time = datetime.now()
        
        # Fetch service principals and sign-in data concurrently
        sp_task = fetch_service_principals_async(tenant_id)
        signin_task = fetch_last_signin_lookup_async(tenant_id)
        
        service_principals, signin_lookup = await asyncio.gather(sp_task, signin_task)
        
        print(f"DEBUG: Async fetched {len(service_principals)} SPs and sign-in data in {(datetime.now() - start_time).total_seconds():.2f}s")
        
        # Transform data (CPU-bound, keep synchronous)
        transform_start = datetime.now()
        records = analyze_and_transform_service_principal_records_optimized(
            service_principals, tenant_id, signin_lookup
        )
        print(f"DEBUG: Transformed data in {(datetime.now() - transform_start).total_seconds():.2f}s")
        
        # Async database storage
        db_start = datetime.now()
        await upsert_many_async('service_principals', records)
        print(f"DEBUG: Async database operations completed in {(datetime.now() - db_start).total_seconds():.2f}s")
        
        total_time = (datetime.now() - start_time).total_seconds()
        print(f"DEBUG: Total async sync time for {tenant_name}: {total_time:.2f}s")
        
        return {
            'status': 'success',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'service_principals_synced': len(records),
            'sync_time_seconds': total_time
        }

    except Exception as e:
        print(f"ERROR: Async service principal sync failed for {tenant_name}: {str(e)}")
        return {
            'status': 'error',
            'tenant_id': tenant_id,
            'tenant_name': tenant_name,
            'error': str(e)
        }

async def sync_service_principals_async_parallel(tenants, max_workers=5):
    """Async parallel processing of multiple tenants with higher concurrency"""
    print(f"DEBUG: Starting async parallel sync for {len(tenants)} tenants")
    
    # Create semaphore to limit concurrent tenants
    semaphore = asyncio.Semaphore(max_workers)
    
    async def bounded_sync(tenant):
        async with semaphore:
            return await sync_service_principals_async(tenant['tenant_id'], tenant['name'])
    
    # Execute all tenant syncs concurrently
    start_time = datetime.now()
    results = await asyncio.gather(*[bounded_sync(tenant) for tenant in tenants], return_exceptions=True)
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Process results
    processed_results = []
    for tenant, result in zip(tenants, results):
        if isinstance(result, Exception):
            print(f"✗ {tenant['name']}: {str(result)}")
            processed_results.append({
                'status': 'error',
                'tenant_id': tenant['tenant_id'],
                'tenant_name': tenant['name'],
                'error': str(result)
            })
        else:
            processed_results.append(result)
            if result['status'] == 'success':
                print(f"✓ {tenant['name']}: {result['service_principals_synced']} SPs synced in {result.get('sync_time_seconds', 0):.2f}s")
            else:
                print(f"✗ {tenant['name']}: {result['error']}")
    
    # Summary
    successful = [r for r in processed_results if r['status'] == 'success']
    failed = [r for r in processed_results if r['status'] == 'error']
    total_synced = sum(r.get('service_principals_synced', 0) for r in successful)
    
    print(f"Async parallel sync completed in {total_time:.2f}s: {len(successful)} tenants successful, {len(failed)} failed, {total_synced} total SPs synced")
    
    return processed_results

def sync_service_principals_async_wrapper(tenants, max_workers=5):
    """Synchronous wrapper for async function to maintain compatibility"""
    return asyncio.run(sync_service_principals_async_parallel(tenants, max_workers))


if __name__ == "__main__":
    """Allow running this module directly to test sync functionality"""
    print("🚀 Running Service Principal Sync Module")
    print("=" * 50)
    
    # Load environment settings
    import core.environment
    
    # Import tenant manager
    from core.tenant_manager import get_tenants
    
    try:
        # Get tenants
        tenants = get_tenants()
        print(f"Found {len(tenants)} tenant(s)")
        
        if not tenants:
            print("❌ No tenants configured. Please check your tenant configuration.")
            exit(1)
        
        # Show available sync options
        print("\nSync Options:")
        print("1. Synchronous sync (original)")
        print("2. Asynchronous sync (optimized)")
        
        # Default to sync version for compatibility
        choice = input("\nSelect option (1 or 2) [default: 1]: ").strip() or "1"
        
        if choice == "2":
            print("\n⚡ Running ASYNC Service Principal Sync...")
            from datetime import datetime
            start_time = datetime.now()
            results = sync_service_principals_async_wrapper(tenants, max_workers=5)
            total_time = (datetime.now() - start_time).total_seconds()
            
            successful = [r for r in results if r['status'] == 'success']
            failed = [r for r in results if r['status'] == 'error']
            total_synced = sum(r.get('service_principals_synced', 0) for r in successful)
            
            print(f"\n📊 ASYNC Results:")
            print(f"   ✅ Successful: {len(successful)}/{len(tenants)} tenants")
            print(f"   ❌ Failed: {len(failed)} tenants")
            print(f"   📈 Total synced: {total_synced} service principals")
            print(f"   ⏱️  Total time: {total_time:.2f} seconds")
            
        else:
            print("\n🔄 Running SYNCHRONOUS Service Principal Sync...")
            from datetime import datetime
            start_time = datetime.now()
            results = sync_service_principals_parallel(tenants, max_workers=3)
            total_time = (datetime.now() - start_time).total_seconds()
            
            successful = [r for r in results if r['status'] == 'success']
            failed = [r for r in results if r['status'] == 'error']
            total_synced = sum(r.get('service_principals_synced', 0) for r in successful)
            
            print(f"\n📊 SYNC Results:")
            print(f"   ✅ Successful: {len(successful)}/{len(tenants)} tenants")
            print(f"   ❌ Failed: {len(failed)} tenants")
            print(f"   📈 Total synced: {total_synced} service principals")
            print(f"   ⏱️  Total time: {total_time:.2f} seconds")
        
        # Show any failures
        if failed:
            print(f"\n❌ Failed tenants:")
            for result in failed:
                print(f"   - {result.get('tenant_name', 'Unknown')}: {result.get('error', 'Unknown error')}")
        
        print("\n✅ Sync completed! Database should be created/updated.")
        
    except Exception as e:
        print(f"❌ Error running sync: {e}")
        import traceback
        traceback.print_exc()
        exit(1)