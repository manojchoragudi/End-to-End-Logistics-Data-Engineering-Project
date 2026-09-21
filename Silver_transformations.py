# Databricks notebook source
# MAGIC %sql
# MAGIC create schema logistics_cata.silver

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


customers = spark.table("logistics_cata.bronze.customers")

customers.printSchema()
display(customers.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

silver_customers = (
    customers

    # Remove duplicate customer records
    .dropDuplicates(["customer_id"])

    # Clean string columns
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("customer_type", F.trim(F.col("customer_type")))

    # Replace missing city
    .withColumn(
        "city",
        F.when(
            F.col("city").isNull() | (F.col("city") == ""),
            F.lit("Unknown")
        ).otherwise(F.col("city"))
    )

    # Convert registration date
    .withColumn(
        "registration_date",
        F.to_date(F.col("registration_date"))
    )

    # Add processing timestamp
    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

display(silver_customers)

# COMMAND ----------

# DBTITLE 1,Cell 6
path = "abfss://silver@adlslogistics.dfs.core.windows.net/customers"

(
    silver_customers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

spark.sql(f"CREATE TABLE IF NOT EXISTS logistics_cata.silver.customers USING DELTA LOCATION '{path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM logistics_cata.silver.customers
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL logistics_cata.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select 
# MAGIC customer_id, count(*) as record_count
# MAGIC from logistics_cata.silver.customers
# MAGIC group by customer_id
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as missing_customer_id
# MAGIC from logistics_cata.silver.customers
# MAGIC where customer_id is  not null
# MAGIC OR customer_id == '';

# COMMAND ----------

warehouses = spark.table("logistics_cata.bronze.warehouses")

silver_warehouses = (
    warehouses

    .dropDuplicates(["warehouse_id"])

    .withColumn("warehouse_id", F.trim(F.col("warehouse_id")))
    .withColumn("warehouse_name", F.trim(F.col("warehouse_name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("warehouse_type", F.trim(F.col("warehouse_type")))

    .withColumn(
        "capacity",
        F.col("capacity").cast("double")
    )

    .withColumn(
        "city",
        F.when(
            F.col("city").isNull() | (F.col("city") == ""),
            "Unknown"
        ).otherwise(F.col("city"))
    )

    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

path = "abfss://silver@adlslogistics.dfs.core.windows.net/warehouses"

(
    silver_warehouses.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

spark.sql(f"CREATE TABLE IF NOT EXISTS logistics_cata.silver.warehouses USING DELTA LOCATION '{path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table logistics_cata.silver.warehouses

# COMMAND ----------

# MAGIC %sql
# MAGIC select warehouse_id, count(*) as record_count
# MAGIC from logistics_cata.silver.warehouses
# MAGIC group by warehouse_id
# MAGIC having count(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as missing_warehouse_id
# MAGIC from logistics_cata.silver.warehouses
# MAGIC where warehouse_id is null
# MAGIC OR warehouse_id == '';

# COMMAND ----------

drivers = spark.table("logistics_cata.bronze.drivers")

silver_drivers = (
    drivers

    .dropDuplicates(["driver_id"])

    .withColumn("driver_id", F.trim(F.col("driver_id")))
    .withColumn("driver_name", F.trim(F.col("driver_name")))
    .withColumn("vehicle_id", F.trim(F.col("vehicle_id")))
    .withColumn("warehouse_id", F.trim(F.col("warehouse_id")))

    .withColumn(
        "experience_years",
        F.col("experience_years").cast("int")
    )

    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

path = "abfss://silver@adlslogistics.dfs.core.windows.net/drivers"

(
    silver_drivers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

spark.sql(f"CREATE TABLE IF NOT EXISTS logistics_cata.silver.drivers USING DELTA LOCATION '{path}'")

# COMMAND ----------

vehicles = spark.table("logistics_cata.bronze.vehicles")

silver_vehicles = (
    vehicles

    .dropDuplicates(["vehicle_id"])

    .withColumn("vehicle_id", F.trim(F.col("vehicle_id")))
    .withColumn("vehicle_type", F.trim(F.col("vehicle_type")))
    .withColumn("fuel_type", F.trim(F.col("fuel_type")))

    .withColumn(
        "vehicle_capacity_kg",
        F.col("vehicle_capacity_kg").cast("double")
    )

    .withColumn(
        "registration_date",
        F.to_date(F.col("registration_date"))
    )

    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

path = "abfss://silver@adlslogistics.dfs.core.windows.net/vehicles"

(
    silver_vehicles.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

spark.sql(f"CREATE TABLE IF NOT EXISTS logistics_cata.silver.vehicles USING DELTA LOCATION '{path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logistics_cata.bronze.orders

# COMMAND ----------

orders = spark.table("logistics_cata.bronze.orders")

silver_orders = (
    orders

    .dropDuplicates(["order_id"])

    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("priority", F.trim(F.col("priority")))
    .withColumn("origin_warehouse", F.trim(F.col("origin_warehouse")))
    .withColumn("destination_city", F.trim(F.col("destination_city")))
    .withColumn("destination_state", F.trim(F.col("destination_state")))

    .withColumn(
        "order_value",
        F.col("order_value").cast("double")
    )

    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"))
    )

    .withColumn(
        "updated_at",
        F.to_timestamp(F.col("updated_at"))
    )

    # Invalid negative order values become NULL
    .withColumn(
        "order_value",
        F.when(
            F.col("order_value") >= 0,
            F.col("order_value")
        ).otherwise(F.lit(None))
    )

    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

df =  spark.table("logistics_cata.bronze.orders")

# COMMAND ----------

display(df)

# COMMAND ----------

df.dropDuplicates(["order_id"])

# COMMAND ----------

df.withColumn("order_id",trim(col("order_id")))

# COMMAND ----------

df.withColumn("order_date",to_date(col("order_date")))

# COMMAND ----------

df.withColumn("silver_processed_timestamp",current_timestamp())

# COMMAND ----------

df.withColumn("order_value", when(col("order_value") >=0, col("order_value")).otherwise(lit(None)))

# COMMAND ----------

path = "abfss://silver@adlslogistics.dfs.core.windows.net/orders"

(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

spark.sql(f"CREATE TABLE IF NOT EXISTS logistics_cata.silver.orders USING DELTA LOCATION '{path}'")

# COMMAND ----------

shipments = spark.table("logistics_cata.bronze.shipments")

silver_shipments = (
    shipments

    .dropDuplicates(["shipment_id"])

    .withColumn("shipment_id", F.trim(F.col("shipment_id")))
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("driver_id", F.trim(F.col("driver_id")))
    .withColumn("vehicle_id", F.trim(F.col("vehicle_id")))
    .withColumn("shipment_status", F.trim(F.col("shipment_status")))

    .withColumn(
        "pickup_date",
        F.to_timestamp(F.col("pickup_date"))
    )

    .withColumn(
        "expected_delivery_date",
        F.to_timestamp(F.col("expected_delivery_date"))
    )

    .withColumn(
        "actual_delivery_date",
        F.to_timestamp(F.col("actual_delivery_date"))
    )

    .withColumn(
        "updated_at",
        F.to_timestamp(F.col("updated_at"))
    )

    .withColumn(
        "weight_kg",
        F.col("weight_kg").cast("double")
    )

    # Standardize shipment status
    .withColumn(
        "shipment_status",
        F.initcap(F.lower(F.col("shipment_status")))
    )

    # Calculate delivery duration
    .withColumn(
        "delivery_duration_hours",
        F.when(
            F.col("actual_delivery_date").isNotNull() &
            F.col("pickup_date").isNotNull(),
            (
                F.col("actual_delivery_date").cast("long") -
                F.col("pickup_date").cast("long")
            ) / 3600
        )
    )

    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

path = "abfss://silver@adlslogistics.dfs.core.windows.net/shipments"

(
    silver_shipments.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

spark.sql(f"CREATE TABLE IF NOT EXISTS logistics_cata.silver.shipments USING DELTA LOCATION '{path}'")

# COMMAND ----------

tracking_events = spark.table(
    "logistics_cata.bronze.tracking_events"
)

silver_tracking_events = (
    tracking_events

    .dropDuplicates(["event_id"])

    .withColumn("event_id", F.trim(F.col("event_id")))
    .withColumn("shipment_id", F.trim(F.col("shipment_id")))
    .withColumn("location", F.trim(F.col("location")))
    .withColumn("event_type", F.trim(F.col("event_type")))
    .withColumn("status", F.trim(F.col("status")))

    .withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("event_timestamp"))
    )

    .withColumn(
        "latitude",
        F.col("latitude").cast("double")
    )

    .withColumn(
        "longitude",
        F.col("longitude").cast("double")
    )

    # Validate coordinates
    .withColumn(
        "latitude",
        F.when(
            (F.col("latitude") >= -90) &
            (F.col("latitude") <= 90),
            F.col("latitude")
        )
    )

    .withColumn(
        "longitude",
        F.when(
            (F.col("longitude") >= -180) &
            (F.col("longitude") <= 180),
            F.col("longitude")
        )
    )

    .withColumn(
        "event_type",
        F.initcap(F.lower(F.col("event_type")))
    )

    .withColumn(
        "status",
        F.upper(F.col("status"))
    )

    .withColumn(
        "_silver_processed_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

path = "abfss://silver@adlslogistics.dfs.core.windows.net/tracking_events"

(
    silver_tracking_events.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

spark.sql(f"CREATE TABLE IF NOT EXISTS logistics_cata.silver.tracking_events USING DELTA LOCATION '{path}'")