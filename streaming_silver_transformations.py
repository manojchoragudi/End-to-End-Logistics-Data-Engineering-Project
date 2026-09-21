# Databricks notebook source
from pyspark.sql import functions as F

bronze_path = (
    "/Volumes/logistics_cata/bronze/"
    "my_volume/volume_direct/streaming_tracking_events/"
)

silver_path = (
    "/Volumes/logistics_cata/bronze/"
    "my_volume/volume_direct/streaming_tracking_events_silver/"
)

silver_checkpoint = (
    "/Volumes/logistics_cata/bronze/"
    "my_volume/volume_direct/checkpoints/streaming_tracking_events_silver"
)

# COMMAND ----------

silver_stream = (
    spark.readStream
    .format("delta")
    .load(
        "/Volumes/logistics_cata/bronze/"
        "my_volume/volume_direct/streaming_tracking_events/"
    )
)

# COMMAND ----------

silver_dupe = silver_stream.dropDuplicates(["event_id"])

# COMMAND ----------

silver_stream.withColumn("event_timestamp",F.to_timestamp("event_timestamp"))

# COMMAND ----------

silver_stream.withColumn("event_type", F.initcap(F.trim("event_type")))

# COMMAND ----------

silver_stream.withColumn(
        "location",
        F.trim(F.col("location")))

# COMMAND ----------

silver_stream.withColumn(
        "longitude",
        F.when(
            (F.col("longitude") >= -180) &
            (F.col("longitude") <= 180),
            F.col("longitude")
        )
    )

# COMMAND ----------

silver_stream.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )

# COMMAND ----------

silver_stream_query = (
    silver_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        silver_checkpoint
    )
    .trigger(availableNow=True)
    .start(silver_path)
)

# COMMAND ----------

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

print("Silver event count:", silver_df.count())

# COMMAND ----------

display(
    spark.read
    .format("delta")
    .load(silver_path)
    .select(
        "event_id",
        "shipment_id",
        "event_timestamp",
        "location",
        "latitude",
        "longitude",
        "event_type",
        "status"
    )
    .limit(20)
)

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN logistics_cata.silver
    """)
)

# COMMAND ----------

streaming_tracking_df = (
    spark.read
    .format("delta")
    .load(
        "/Volumes/logistics_cata/bronze/"
        "my_volume/volume_direct/"
        "streaming_tracking_events_silver/"
    )
)

display(streaming_tracking_df.limit(10))

# COMMAND ----------

batch_shipments = spark.table(
    "logistics_cata.silver.shipments"
)

display(batch_shipments.limit(10))

# COMMAND ----------

streaming_tracking = (
    spark.read
    .format("delta")
    .load(
        "/Volumes/logistics_cata/bronze/"
        "my_volume/volume_direct/"
        "streaming_tracking_events_silver/"
    )
)

display(streaming_tracking.limit(10))

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