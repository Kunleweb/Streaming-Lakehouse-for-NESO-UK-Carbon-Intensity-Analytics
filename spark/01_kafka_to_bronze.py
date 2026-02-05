from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from delta import configure_spark_with_delta_pip
from schemas import carbon_schema

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "carbon.intensity.uk"

import os

# Windows Environment Setup
if os.name == 'nt':
    if 'JAVA_HOME' not in os.environ:
        # Default to the path we just installed
        known_java = r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
        if os.path.exists(known_java):
            os.environ['JAVA_HOME'] = known_java
            print(f"Set JAVA_HOME to {known_java}")
    
    if 'HADOOP_HOME' not in os.environ:
        known_hadoop = r"c:\hadoop"
        if os.path.exists(known_hadoop):
            os.environ['HADOOP_HOME'] = known_hadoop
            # Also add bin to PATH for hadoop.dll
            hadoop_bin = os.path.join(known_hadoop, 'bin')
            if hadoop_bin not in os.environ['PATH']:
                 os.environ['PATH'] += ";" + hadoop_bin
            print(f"Set HADOOP_HOME to {known_hadoop} and updated PATH")

BRONZE_PATH = "data/delta/bronze_carbon_intensity"
CHECKPOINT_PATH = "data/delta/_checkpoints/bronze_carbon_intensity"

builder = (
    SparkSession.builder.appName("kafka_to_bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Windows filesystem config
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.util.NativeCodeLoader.disable", "true")

    .config("spark.local.dir", "C:/spark-tmp")
)


# Configure Spark with Delta and Kafka
# Using 3.5.3 for compatibility
spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Kafka value is bytes; parse JSON
parsed = (
    raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), carbon_schema).alias("data"))
    .select("data.*")
)

query = (
    parsed.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(BRONZE_PATH)
)

print("Kafka to Bronze started. Ctrl+C to stop.")
query.awaitTermination()
