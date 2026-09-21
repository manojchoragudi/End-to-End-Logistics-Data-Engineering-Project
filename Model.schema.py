# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

customers = spark.table("logistics_cata.silver.customers")

customers.printSchema()
display(customers.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim_Customers**

# COMMAND ----------

dim_customers = customers.select('customer_id', 'customer_name','city','state', 'country','customer_type','registration_date')\
                        .dropDuplicates(['customer_id'])

# COMMAND ----------

display(dim_customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema logistics_cata.gold

# COMMAND ----------

# DBTITLE 1,Create Gold External Volume
# MAGIC %sql
# MAGIC DROP VOLUME IF EXISTS logistics_cata.gold.gold_volume;
# MAGIC CREATE EXTERNAL VOLUME logistics_cata.gold.gold_volume
# MAGIC LOCATION 'abfss://gold@adlslogistics.dfs.core.windows.net/volumes/';

# COMMAND ----------

# DBTITLE 1,Write dim_customers to Gold
dim_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/dim_customers") \
    .saveAsTable("logistics_cata.gold.dim_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM logistics_cata.gold.dim_customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

drivers = spark.table("logistics_cata.silver.drivers")

drivers.printSchema()
display(drivers.limit(10))


# COMMAND ----------

dim_drivers = drivers.select("driver_id", "driver_name","_silver_processed_timestamp","warehouse_id")\
                        .dropDuplicates(['driver_id'])
display(dim_drivers)


# COMMAND ----------

dim_drivers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/dim_drivers") \
    .saveAsTable("logistics_cata.gold.dim_drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim_vehicles**

# COMMAND ----------

vehicles = spark.table("logistics_cata.silver.vehicles")

vehicles.printSchema()

# COMMAND ----------

dim_vehicles = (
    vehicles
    .select(
        "vehicle_id",
        "vehicle_type",
        "vehicle_number"
    )
    .filter("vehicle_id IS NOT NULL")
    .dropDuplicates(["vehicle_id"])
)

dim_drivers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/dim_vehicles") \
    .saveAsTable("logistics_cata.gold.dim_vehicles")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim_Warehouses**

# COMMAND ----------

warehouses = spark.table("logistics_cata.silver.warehouses")

warehouses.printSchema()

# COMMAND ----------

dim_warehouses = (
    warehouses
    .select(
        "warehouse_id",
        "warehouse_name",
        "city",
        "state",
        "country"
    )
    .filter("warehouse_id IS NOT NULL")
    .dropDuplicates(["warehouse_id"])
)

dim_drivers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/dim_warehouses") \
    .saveAsTable("logistics_cata.gold.dim_warehouses")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists logistics_cata.gold.dim_warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim_date**

# COMMAND ----------

from pyspark.sql import functions as F

dim_date = (
    spark.range(
        0,
        4018
    )
    .withColumn(
        "date",
        F.date_add(F.to_date(F.lit("2020-01-01")), F.col("id").cast("int"))
    )
    .select("date")
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .drop("id")
)

# COMMAND ----------

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/dim_date") \
    .saveAsTable("logistics_cata.gold.dim_date")

# COMMAND ----------

for table in [
    "dim_customers",
    "dim_drivers",
    "dim_vehicles",
    "dim_warehouses",
    "dim_date"
]:
    count = spark.table(f"logistics_cata.gold.{table}").count()
    print(f"{table}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fact_Orders**

# COMMAND ----------

orders = spark.table("logistics_cata.silver.orders")

orders.printSchema()
display(orders.limit(10))

# COMMAND ----------

fact_orders = (
    orders
    .select(
        col("order_id"),
        col("customer_id"),
        col("order_date").cast("date").alias("order_date"),
        col("order_value"),
        col("priority"),
        col("origin_warehouse").alias("warehouse_id"),
        col("destination_city"),
        col("destination_state"),
        col("updated_at").cast("timestamp").alias("updated_at")
    )
    .filter(col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

# COMMAND ----------

fact_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/fact_orders") \
    .saveAsTable("logistics_cata.gold.fact_orders")

# COMMAND ----------

print(
    "fact_orders:",
    spark.table("logistics_cata.gold.fact_orders").count()
)

# COMMAND ----------

shipments = spark.table("logistics_cata.silver.shipments")

shipments.printSchema()
display(shipments.limit(10))

# COMMAND ----------

fact_shipments = (
    shipments
    .select(
        col("shipment_id"),
        col("order_id"),
        col("driver_id"),
        col("vehicle_id"),
        col("pickup_date"),
        col("expected_delivery_date"),
        col("actual_delivery_date"),
        col("shipment_status"),
        col("weight_kg"),
        col("delivery_duration_hours"),
        col("updated_at")
    )
    .filter(col("shipment_id").isNotNull())
    .dropDuplicates(["shipment_id"])
)

# COMMAND ----------

fact_shipments.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/fact_shipments") \
    .saveAsTable("logistics_cata.gold.fact_shipments")

# COMMAND ----------

streaming_tracking = spark.read.format("delta").load(
    "/Volumes/logistics_cata/bronze/my_volume/volume_direct/streaming_tracking_events_silver/"
)

streaming_tracking.printSchema()
display(streaming_tracking.limit(10))

# COMMAND ----------

fact_tracking_events = (
    streaming_tracking
    .select(
        col("event_id"),
        col("shipment_id"),
        col("event_timestamp"),
        col("location").alias("tracking_location"),
        col("latitude"),
        col("longitude"),
        col("event_type"),
        col("status").alias("tracking_status"),
        col("partition").alias("eventhub_partition"),
        col("offset").alias("eventhub_offset"),
        col("stream_processed_timestamp")
    )
    .filter(col("event_id").isNotNull())
    .dropDuplicates(["event_id"])
)

# COMMAND ----------

fact_tracking_events.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@adlslogistics.dfs.core.windows.net/tables/fact_tracking_events") \
    .saveAsTable("logistics_cata.gold.fact_tracking_events")

# COMMAND ----------

for table in [
    "dim_customers",
    "dim_drivers",
    "dim_vehicles",
    "dim_warehouses",
    "dim_date",
    "fact_orders",
    "fact_shipments",
    "fact_tracking_events"
]:
    print(
        table,
        "=>",
        spark.table(f"logistics_cata.gold.{table}").count()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shipment_overview

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW logistics_cata.gold.vw_shipment_overview AS
SELECT
    s.shipment_id,
    s.order_id,
    o.customer_id,
    s.driver_id,
    s.vehicle_id,
    o.warehouse_id,
    s.pickup_date,
    s.expected_delivery_date,
    s.actual_delivery_date,
    s.shipment_status,
    s.weight_kg,
    s.delivery_duration_hours,
    o.order_value,
    o.priority,
    o.destination_city,
    o.destination_state
FROM logistics_cata.gold.fact_shipments s
LEFT JOIN logistics_cata.gold.fact_orders o
    ON s.order_id = o.order_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delivery Performance

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW logistics_cata.gold.vw_delivery_performance AS
SELECT
    shipment_status,
    COUNT(*) AS total_shipments,
    ROUND(AVG(weight_kg), 2) AS avg_weight_kg,
    ROUND(AVG(delivery_duration_hours), 2) AS avg_delivery_hours,
    SUM(
        CASE
            WHEN actual_delivery_date IS NOT NULL
                 AND actual_delivery_date <= expected_delivery_date
            THEN 1
            ELSE 0
        END
    ) AS on_time_shipments,
    SUM(
        CASE
            WHEN actual_delivery_date IS NOT NULL
                 AND actual_delivery_date > expected_delivery_date
            THEN 1
            ELSE 0
        END
    ) AS delayed_shipments
FROM logistics_cata.gold.fact_shipments
GROUP BY shipment_status
""")

# COMMAND ----------

display(
    spark.table("logistics_cata.gold.vw_delivery_performance")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Orders

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW logistics_cata.gold.vw_customer_orders AS
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.city,
    c.state,
    c.country,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(o.order_value), 2) AS total_order_value,
    ROUND(AVG(o.order_value), 2) AS average_order_value
FROM logistics_cata.gold.dim_customers c
LEFT JOIN logistics_cata.gold.fact_orders o
    ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.city,
    c.state,
    c.country
""")