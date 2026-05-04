# ============================================================
# Connect Databricks to Azure Blob Storage
# ============================================================

STORAGE_ACCOUNT     = "sthealthcaredwh01"
STORAGE_KEY         = "Access_key"   
LANDING_CONTAINER   = "landing"
BRONZE_CONTAINER    = "bronze"

# Authenticate Spark to Blob Storage
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    STORAGE_KEY
)

# Build paths
LANDING_PATH = f"wasbs://{LANDING_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/"
BRONZE_PATH  = f"wasbs://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/"

print("✅ Storage connected!")
print(f"   Landing : {LANDING_PATH}")
print(f"   Bronze  : {BRONZE_PATH}")
