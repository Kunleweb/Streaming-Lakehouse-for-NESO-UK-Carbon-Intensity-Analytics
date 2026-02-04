from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from delta import configure_spark_with_delta_pip
from schemas import carbon_schema

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "carbon.intensity.uk"

BRONZE_PATH = "data/delta/bronze_carbon_intensity"
CHECKPOINT_PATH = "data/delta/_checkpoints/bronze_carbon_intensity"

builder = (
    SparkSession.builder.appName("kafka_to_bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
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
