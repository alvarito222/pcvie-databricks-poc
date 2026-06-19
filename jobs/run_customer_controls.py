import sys
import inspect
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lower,
    trim,
    concat_ws,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

def get_current_file_path() -> Path:
    """
    Compatible Databricks Job / Git source.
    Dans certains contextes Databricks, __file__ n'est pas défini.
    On récupère alors le filename utilisé par compile(...).
    """
    if "__file__" in globals():
        return Path(__file__).resolve()

    frame = inspect.currentframe()
    if frame is not None:
        return Path(frame.f_code.co_filename).resolve()

    raise RuntimeError("Impossible de déterminer le chemin du script courant.")


def find_src_path(start_path: Path) -> Path:
    """
    Recherche le dossier src du repo.
    Structure attendue :
      repo/
        jobs/run_customer_controls.py
        src/pcvie_controls/
    """
    for parent in [start_path.parent, *start_path.parents]:
        candidate = parent / "src" / "pcvie_controls"
        if candidate.exists():
            return parent / "src"

    raise RuntimeError(
        f"Impossible de trouver src/pcvie_controls depuis {start_path}"
    )


current_file = get_current_file_path()
src_path = find_src_path(current_file)

print(f"Current file detected: {current_file}")
print(f"Source path detected: {src_path}")

sys.path.insert(0, str(src_path))

from pcvie_controls.scoring import control_customer


spark = SparkSession.builder.getOrCreate()


CATALOG = "main"
SOURCE_SCHEMA = "default"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.customers"
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.customers_clean"
GOLD_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.customer_control_results"


def create_schemas():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")


def build_bronze():
    df = (
        spark.table(SOURCE_TABLE)
        .withColumn("bronze_ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit("EasyLife-POC"))
    )

    df.write.mode("overwrite").saveAsTable(BRONZE_TABLE)


def build_silver():
    df = (
        spark.table(BRONZE_TABLE)
        .select(
            col("customer_id").cast("int").alias("customer_id"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            col("created_at").cast("timestamp").alias("created_at"),
            col("bronze_ingestion_timestamp"),
            col("source_system"),
        )
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        .filter(col("customer_id").isNotNull())
    )

    df.write.mode("overwrite").saveAsTable(SILVER_TABLE)


control_schema = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("customer_score", IntegerType(), True),
        StructField("email_quality", StringType(), True),
        StructField("control_status", StringType(), True),
        StructField("control_errors", StringType(), True),
        StructField("algorithm_version", StringType(), True),
    ]
)


@udf(returnType=control_schema)
def control_customer_udf(customer_id, first_name, last_name, email):
    return control_customer(
        {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
        }
    )


def build_gold():
    source_df = spark.table(SILVER_TABLE)

    controlled_df = source_df.withColumn(
        "control",
        control_customer_udf(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
        ),
    )

    result_df = controlled_df.select(
        col("control.customer_id").alias("customer_id"),
        col("control.full_name").alias("full_name"),
        col("control.email").alias("email"),
        col("control.customer_score").alias("customer_score"),
        col("control.email_quality").alias("email_quality"),
        col("control.control_status").alias("control_status"),
        col("control.control_errors").alias("control_errors"),
        col("control.algorithm_version").alias("algorithm_version"),
        current_timestamp().alias("control_execution_timestamp"),
    )

    result_df.write.mode("overwrite").saveAsTable(GOLD_TABLE)


def main():
    create_schemas()
    build_bronze()
    build_silver()
    build_gold()

    print("POC completed successfully.")
    print(f"Bronze table: {BRONZE_TABLE}")
    print(f"Silver table: {SILVER_TABLE}")
    print(f"Gold table: {GOLD_TABLE}")


if __name__ == "__main__":
    main()