# Databricks notebook source
from pyspark.sql import functions as F

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

# DBTITLE 1,Cell 2
shipment_tracking_gold = (
    streaming_tracking.alias("t")
    .join(
        batch_shipments.alias("s"),
        F.col("t.shipment_id") == F.col("s.shipment_id"),
        "left"
    )
    .join(
        spark.table("logistics_cata.silver.orders").alias("o"),
        F.col("s.order_id") == F.col("o.order_id"),
        "left"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 5
shipment_tracking_gold = (
    shipment_tracking_gold
    .select(
        # Tracking event
        F.col("t.event_id").alias("event_id"),
        F.col("t.shipment_id").alias("shipment_id"),

        # Order / customer information
        F.col("s.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.origin_warehouse").alias("warehouse_id"),

        # Shipment information
        F.col("s.driver_id").alias("driver_id"),
        F.col("s.vehicle_id").alias("vehicle_id"),

        # Streaming tracking information
        F.col("t.event_timestamp").alias("event_timestamp"),
        F.col("t.location").alias("tracking_location"),
        F.col("t.latitude").alias("latitude"),
        F.col("t.longitude").alias("longitude"),
        F.col("t.event_type").alias("event_type"),
        F.col("t.status").alias("tracking_status"),

        # Event Hubs metadata
        F.col("t.partition").alias("eventhub_partition"),
        F.col("t.offset").alias("eventhub_offset"),
        F.col("t.stream_processed_timestamp").alias("stream_processed_timestamp")
    )
)

# COMMAND ----------

display(shipment_tracking_gold.limit(20))

# COMMAND ----------

batch_shipments.select(
    "shipment_id",
    "order_id",
    "driver_id",
    "vehicle_id"
).show(20, False)

# COMMAND ----------

streaming_tracking.select(
    "shipment_id"
).distinct().show(20, False)

# COMMAND ----------

join_test = (
    streaming_tracking.alias("t")
    .join(
        batch_shipments.alias("s"),
        F.col("t.shipment_id") == F.col("s.shipment_id"),
        "inner"
    )
)

print("Matching shipment records:", join_test.count())

# COMMAND ----------

real_shipment_ids = (
    spark.table("logistics_cata.silver.shipments")
    .select("shipment_id")
    .distinct()
    .limit(100)
    .collect()
)

shipment_ids = [row["shipment_id"] for row in real_shipment_ids]

print(shipment_ids[:10])

# COMMAND ----------

shipment_tracking_gold = (
    streaming_tracking.alias("t")
    .join(
        batch_shipments.alias("s"),
        F.col("t.shipment_id") == F.col("s.shipment_id"),
        "inner"
    )
    .join(
        spark.table("logistics_cata.silver.orders").alias("o"),
        F.col("s.order_id") == F.col("o.order_id"),
        "left"
    )
    .select(
        F.col("t.event_id").alias("event_id"),
        F.col("t.shipment_id").alias("shipment_id"),
        F.col("s.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.origin_warehouse").alias("warehouse_id"),
        F.col("s.driver_id").alias("driver_id"),
        F.col("s.vehicle_id").alias("vehicle_id"),
        F.col("t.event_timestamp").alias("event_timestamp"),
        F.col("t.location").alias("tracking_location"),
        F.col("t.latitude").alias("latitude"),
        F.col("t.longitude").alias("longitude"),
        F.col("t.event_type").alias("event_type"),
        F.col("t.status").alias("tracking_status"),
        F.col("t.partition").alias("eventhub_partition"),
        F.col("t.offset").alias("eventhub_offset"),
        F.col("t.stream_processed_timestamp").alias("stream_processed_timestamp")
    )
)

# COMMAND ----------

print("Gold matching records:", shipment_tracking_gold.count())

display(
    shipment_tracking_gold
    .orderBy(F.col("event_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

from pyspark.sql import functions as F

batch_shipments = spark.table("logistics_cata.silver.shipments")
orders = spark.table("logistics_cata.silver.orders")
streaming_tracking = spark.read.format("delta").load(
    "/Volumes/logistics_cata/bronze/my_volume/volume_direct/streaming_tracking_events_silver/"
)

gold_shipment_tracking = (
    streaming_tracking.alias("t")
    .join(
        batch_shipments.alias("s"),
        F.col("t.shipment_id") == F.col("s.shipment_id"),
        "inner"
    )
    .join(
        orders.alias("o"),
        F.col("s.order_id") == F.col("o.order_id"),
        "left"
    )
    .select(
        F.col("t.event_id").alias("event_id"),
        F.col("t.shipment_id").alias("shipment_id"),
        F.col("s.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.origin_warehouse").alias("warehouse_id"),
        F.col("s.driver_id").alias("driver_id"),
        F.col("s.vehicle_id").alias("vehicle_id"),
        F.col("t.event_timestamp").alias("event_timestamp"),
        F.col("t.location").alias("tracking_location"),
        F.col("t.latitude").alias("latitude"),
        F.col("t.longitude").alias("longitude"),
        F.col("t.event_type").alias("event_type"),
        F.col("t.status").alias("tracking_status"),
        F.col("t.partition").alias("eventhub_partition"),
        F.col("t.offset").alias("eventhub_offset"),
        F.col("t.stream_processed_timestamp").alias(
            "stream_processed_timestamp"
        )
    )
)

# COMMAND ----------

print("Gold records:", gold_shipment_tracking.count())

# COMMAND ----------

display(
    gold_shipment_tracking
    .orderBy(F.col("event_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

display(
    gold_shipment_tracking.select(
        "shipment_id",
        "order_id",
        "customer_id",
        "warehouse_id",
        "driver_id",
        "vehicle_id",
        "event_type",
        "tracking_status"
    ).limit(20)
)