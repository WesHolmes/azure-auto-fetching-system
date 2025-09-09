-- Query to get all Azure devices
sqlite3 db/sqlite.db "SELECT * FROM azure_devices LIMIT 10;";

-- Query devices with no storage available
sqlite3 db/sqlite.db "SELECT COUNT(*) FROM intune_devices WHERE tenant_id = '3aae0fb1-276f-42f8-8e4d-36ca10cbb779' AND (free_storage_gb = 0);";