# ============================================================
# Bronze Ingestion: Landing → Bronze (Delta)
# ============================================================
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

# Define all 9 datasets
datasets = [
    "patients",
    "encounters",
    "diagnoses",
    "claims_and_billing",
    "denials",
    "procedures",
    "medications",
    "providers",
    "lab_tests"
]

ingestion_time = current_timestamp()
results = []

for name in datasets:
    try:
        # Read CSV from landing
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(f"{LANDING_PATH}{name}.csv")

        row_count = df.count()

        # Add audit columns
        df = df \
            .withColumn("_ingested_at", ingestion_time) \
            .withColumn("_source_file", lit(f"{name}.csv")) \
            .withColumn("_layer", lit("bronze"))

        # Write as Delta to bronze container
        output_path = f"{BRONZE_PATH}{name}"
        df.write.format("delta") \
            .mode("overwrite") \
            .save(output_path)

        results.append((name, row_count, "SUCCESS"))
        print(f" {name:<25} {row_count:>7,} rows → bronze/{name}")

    except Exception as e:
        results.append((name, 0, f" FAILED: {e}"))
        print(f" {name:<25} FAILED: {e}")

print(f"\n Bronze ingestion complete — {sum(1 for r in results if '' in r[2])}/9 tables loaded")
