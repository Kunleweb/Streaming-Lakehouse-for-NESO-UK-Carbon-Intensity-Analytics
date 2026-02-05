from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip, DeltaTable

BRONZE_PATH = "data/delta/bronze_carbon_intensity"
SILVER_PATH = "data/delta/silver_carbon_intensity"
CHECKPOINT_PATH = "data/delta/_checkpoints/silver_carbon_intensity"

import os

# ---- Environment Checks for Windows ----
if os.name == 'nt':
    if 'JAVA_HOME' not in os.environ:
        known_java = r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
        if os.path.exists(known_java):
            os.environ['JAVA_HOME'] = known_java
    
    if 'HADOOP_HOME' not in os.environ:
        known_hadoop = r"c:\hadoop"
        if os.path.exists(known_hadoop):
            os.environ['HADOOP_HOME'] = known_hadoop
            hadoop_bin = os.path.join(known_hadoop, 'bin')
            if hadoop_bin not in os.environ['PATH']:
                 os.environ['PATH'] += ";" + hadoop_bin

builder = (
    SparkSession.builder.appName("bronze_to_silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # ---- Windows local filesystem fixes ----
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.util.NativeCodeLoader.disable", "true")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.readStream.format("delta").load(BRONZE_PATH)

# Convert fetched_at to timestamp for ordering
with_ts = bronze.withColumn("fetched_at_ts", to_timestamp(col("fetched_at")))

# Window definition moved inside upsert_to_silver because it's not supported on streaming DataFrames

def upsert_to_silver(microbatch_df, batch_id: int):
    # Keep latest record per event_id per micro-batch
    # (Window functions are valid here on the static microbatch_df)
    w = Window.partitionBy("event_id").orderBy(col("fetched_at_ts").desc())
    microbatch_df = (
        microbatch_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    # Create table if not exists
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        microbatch_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
        return

    silver = DeltaTable.forPath(spark, SILVER_PATH)

    (
        silver.alias("t")
        .merge(
            microbatch_df.alias("s"),
            "t.event_id = s.event_id"
        )
        .whenMatchedUpdate(set={
            "from_ts": "s.from_ts",
            "to_ts": "s.to_ts",
            "forecast_gco2_kwh": "s.forecast_gco2_kwh",
            "actual_gco2_kwh": "s.actual_gco2_kwh",
            "index": "s.index",
            "fetched_at": "s.fetched_at",
            "fetched_at_ts": "s.fetched_at_ts",
            "source": "s.source",
        })
        .whenNotMatchedInsert(values={
            "event_id": "s.event_id",
            "from_ts": "s.from_ts",
            "to_ts": "s.to_ts",
            "forecast_gco2_kwh": "s.forecast_gco2_kwh",
            "actual_gco2_kwh": "s.actual_gco2_kwh",
            "index": "s.index",
            "fetched_at": "s.fetched_at",
            "fetched_at_ts": "s.fetched_at_ts",
            "source": "s.source",
        })
        .execute()
    )

query = (
    with_ts.writeStream
    .foreachBatch(upsert_to_silver)
    .outputMode("update")  # update/append both work; foreachBatch controls writes
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("Bronze to Silver upsert started. Ctrl+C to stop.")
query.awaitTermination()
