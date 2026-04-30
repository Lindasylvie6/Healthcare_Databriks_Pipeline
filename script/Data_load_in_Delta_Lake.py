# Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS healthcare")

# Connection settings
jdbc_url = "jdbc:sqlserver://sql-healthcare-dwh-db.database.windows.net:1433;database=healthcare-dwh-db"

connection_properties = {
    "user": "CloudSA78da23a3",
    "password": "******",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "true",
    "trustServerCertificate": "true"
}

# List of all gold tables
gold_tables = [
    "gold.fact_monthly_kpis",
    "gold.fact_encounters",
    "gold.fact_claims",
    "gold.fact_clinical",
    "gold.fact_provider_performance",
    "gold.dim_patient",
    "gold.dim_provider",
    "gold.dim_date"
]

# Load each table into Delta
for table in gold_tables:
    delta_name = table.replace("gold.", "gold_")
    print(f"Loading {table}...")
    
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table,
        properties=connection_properties
    )
    
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"healthcare.{delta_name}")
    
    print(f" {delta_name} loaded!")

print("All tables loaded!")

# check tables creation
spark.sql("SHOW TABLES IN healthcare").display()
