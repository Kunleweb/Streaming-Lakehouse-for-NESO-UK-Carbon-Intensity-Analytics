from pyspark.sql.types import StructType, StructField, StringType, DoubleType

carbon_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("from_ts", StringType(), False),
    StructField("to_ts", StringType(), False),
    StructField("forecast_gco2_kwh", DoubleType(), True),
    StructField("actual_gco2_kwh", DoubleType(), True),
    StructField("index", StringType(), True),
    StructField("fetched_at", StringType(), False),
    StructField("source", StringType(), False),
])
