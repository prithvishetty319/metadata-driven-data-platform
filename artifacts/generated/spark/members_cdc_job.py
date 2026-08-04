"""Generated Spark entry point for members_cdc."""
from pyspark.sql import SparkSession, functions as F

PIPELINE_NAME = "members_cdc"
SOURCE_TYPE = "postgres_cdc"
TARGET_TABLE = "healthcare.silver.members_current"
WRITE_MODE = "merge"
KEYS = ['member_id']
COLUMNS = ['member_id', 'plan_id', 'state_code', 'effective_date', 'source_sequence']

def enforce_contract(frame):
    missing = sorted(set(COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"missing contract columns: {missing}")
    return frame.select(*COLUMNS).withColumn("_loaded_at", F.current_timestamp())

def run(spark: SparkSession, source_path: str):
    frame = enforce_contract(spark.read.format("delta").load(source_path))
    if WRITE_MODE == "merge":
        from delta.tables import DeltaTable
        target = DeltaTable.forName(spark, TARGET_TABLE)
        condition = " AND ".join([f"target.{key} = source.{key}" for key in KEYS])
        (target.alias("target").merge(frame.alias("source"), condition)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        frame.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

if __name__ == "__main__":
    session = SparkSession.builder.appName(PIPELINE_NAME).getOrCreate()
    run(session, "{{ source_path }}")
