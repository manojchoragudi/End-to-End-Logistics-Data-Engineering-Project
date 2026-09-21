# Databricks notebook source
EH_POLICY_NAME = "databricks-listen"

EH_CONN_STR = dbutils.secrets.get(
    scope="logistics-secrets",
    key="databricks-listen"
)

print("Event Hubs connection string retrieved")

# COMMAND ----------

EH_NAMESPACE = "logistics-tracking-events"
EH_NAME = "logistics_events"

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":
        f"{EH_NAMESPACE}.servicebus.windows.net:9093",

    "subscribe": EH_NAME,

    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",

    "kafka.sasl.jaas.config":
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{EH_CONN_STR}";',

    "startingOffsets": "earliest",
    "failOnDataLoss": "false",

    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "maxOffsetsPerTrigger": "1000"
}

print("Kafka configuration ready")

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)

# COMMAND ----------

# DBTITLE 1,Cell 4
dbutils.fs.rm("/Volumes/logistics_cata/bronze/my_volume/volume_direct/offsets", True)

query = (
    raw_stream
    .writeStream
    .format("memory")
    .queryName("eventhub_test")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/Volumes/logistics_cata/bronze/my_volume/volume_direct/"
    )
    .start()
)

# COMMAND ----------

display(spark.sql("""
    SELECT
        topic,
        partition,
        offset,
        CAST(value AS STRING) AS value,
        timestamp
    FROM eventhub_test
    ORDER BY offset
"""))

# COMMAND ----------

query.stop()

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)
from pyspark.sql import functions as F

tracking_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("shipment_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("location", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("event_type", StringType(), True),
    StructField("status", StringType(), True)
])

# COMMAND ----------

parsed_stream = (
    raw_stream
    .select(
        F.from_json(
            F.col("value").cast("string"),
            tracking_schema
        ).alias("data"),

        F.col("timestamp").alias("eh_enqueued_timestamp"),
        F.col("partition"),
        F.col("offset")
    )
    .select(
        "data.*",
        "eh_enqueued_timestamp",
        "partition",
        "offset"
    )
    .withColumn(
        "event_timestamp",
        F.to_timestamp("event_timestamp")
    )
    .withColumn(
        "stream_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 10
bronze_data_path = "YOUR_EXISTING_BRONZE_EXTERNAL_LOCATION/streaming_tracking_events"

bronze_checkpoint = (
    "/Volumes/logistics_cata/bronze/my_volume/"
    "volume_direct/checkpoints/streaming_tracking_events"
)

bronze_stream_query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .start("/Volumes/logistics_cata/bronze/my_volume/volume_direct/streaming_tracking_events/")
)

# COMMAND ----------

display(
    spark.sql("""
        SHOW EXTERNAL LOCATIONS
    """)
)


# COMMAND ----------

display(
    spark.read.format("delta")
    .load(
        "/Volumes/logistics_cata/bronze/my_volume/"
        "volume_direct/streaming_tracking_events/"
    )
    .orderBy(F.col("stream_processed_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

streaming_bronze_df = (
    spark.read
    .format("delta")
    .load(
        "/Volumes/logistics_cata/bronze/my_volume/"
        "volume_direct/streaming_tracking_events/"
    )
)

print("Total streaming events:", streaming_bronze_df.count())

# COMMAND ----------

display(
    spark.read.format("delta")
    .load(
        "/Volumes/logistics_cata/bronze/"
        "my_volume/volume_direct/streaming_tracking_events/"
    )
    .select(
        "event_id",
        "shipment_id",
        "event_timestamp",
        "location",
        "latitude",
        "longitude",
        "event_type",
        "status",
        "stream_processed_timestamp"
    )
    .orderBy(F.col("stream_processed_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

bronze_stream_query.stop()

# COMMAND ----------

bronze_stream_query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/Volumes/logistics_cata/bronze/"
        "my_volume/volume_direct/checkpoints/"
        "streaming_tracking_events"
    )
    .trigger(availableNow=True)
    .start(
        "/Volumes/logistics_cata/bronze/"
        "my_volume/volume_direct/streaming_tracking_events/"
    )
)

# COMMAND ----------

display(
    spark.read
    .format("delta")
    .load(
        "/Volumes/logistics_cata/bronze/"
        "my_volume/volume_direct/"
        "streaming_tracking_events/"
    )
    .select(
        "event_id",
        "shipment_id",
        "event_timestamp",
        "event_type",
        "status"
    )
    .orderBy(
        F.col("stream_processed_timestamp").desc()
    )
)

# COMMAND ----------

streaming_tracking = spark.read.format("delta").load(
    "/Volumes/logistics_cata/bronze/my_volume/volume_direct/streaming_tracking_events_silver/"
)

batch_shipments = spark.table("logistics_cata.silver.shipments")

matching = (
    streaming_tracking.alias("t")
    .join(
        batch_shipments.alias("s"),
        F.col("t.shipment_id") == F.col("s.shipment_id"),
        "inner"
    )
)

print("Matching shipment records:", matching.count())

# COMMAND ----------

display(
    streaming_tracking
    .select("shipment_id")
    .distinct()
    .orderBy("shipment_id")
)

# COMMAND ----------

bronze_df = spark.read.format("delta").load(
    "/Volumes/logistics_cata/bronze/my_volume/volume_direct/streaming_tracking_events/"
)

display(
    bronze_df
    .select(
        "event_id",
        "shipment_id",
        "event_type",
        "status",
        "eh_enqueued_timestamp"
    )
    .orderBy(F.col("eh_enqueued_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

display(
    bronze_df
    .filter(
        F.col("shipment_id").startswith("SHP000")
    )
    .select(
        "event_id",
        "shipment_id",
        "event_type",
        "status",
        "eh_enqueued_timestamp"
    )
    .orderBy(F.col("eh_enqueued_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

silver_df = spark.read.format("delta").load(
    "/Volumes/logistics_cata/bronze/my_volume/volume_direct/streaming_tracking_events_silver/"
)

display(
    silver_df
    .filter(
        F.col("shipment_id").startswith("SHP000")
    )
    .select(
        "event_id",
        "shipment_id",
        "event_type",
        "status",
        "event_timestamp"
    )
    .orderBy(F.col("event_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

matching = (
    silver_df.alias("t")
    .join(
        batch_shipments.alias("s"),
        F.col("t.shipment_id") == F.col("s.shipment_id"),
        "inner"
    )
)

print("Matching shipment records:", matching.count())