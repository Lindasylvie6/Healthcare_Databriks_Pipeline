# Connect Azure SQL Database into Databricks DataFrame via JDBC Connection
jdbc_url = "jdbc:sqlserver://sql-healthcare-dwh-db.database.windows.net:1433;database=healthcare-dwh-db"

connection_properties = {
    "user": "CloudSA78da23a3",
    "password": "******",  # your password
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "true",
    "trustServerCertificate": "true"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="gold.fact_monthly_kpis",
    properties=connection_properties
)

df.display()
