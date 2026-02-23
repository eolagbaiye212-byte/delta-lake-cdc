# This script creates the gold layer of the architecture, implementing star schema

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import os
import sys
import pandas as pd

def gold_layer():
    os.environ["HADOOP_HOME"] = "C:\\hadoop" # Path to the folder containing the bin folder
    os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

    # Create Spark session with Delta Lake
    spark = SparkSession.builder.appName("Gold Layer - Star Schema").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.python.worker.reuse", "true").config("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "false").config("spark.network.timeout", "600s").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("="*60)
    print("GOLD LAYER: Putting the Data into Star Schema")
    print("="*60)

    # Read silver data and assign data from each table to a dataframe
    silver_df_customer = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/customers")
    silver_df_products = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/products")
    silver_df_inventory = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/inventory")
    silver_df_orders = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/orders")
    silver_df_order_items = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/order_items")
    silver_df_orders_undeduplicated = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/orders_undeduplicated")

    # Create temporary view of each of the silver dataframes
    silver_df_customer.createOrReplaceTempView("silver_customers")
    silver_df_products.createOrReplaceTempView("silver_products")
    silver_df_inventory.createOrReplaceTempView("silver_inventory")
    silver_df_orders.createOrReplaceTempView("silver_orders")
    silver_df_order_items.createOrReplaceTempView("silver_order_items")
    silver_df_orders_undeduplicated.createOrReplaceTempView("silver_orders_undeduplicated")

    # Create dim_date dimension(pre-generated)
    # Generate dates from 2025 to 2030
    start_date = datetime(2026, 1, 1) # January 1st, 2025 at 00:00:00 (since not explicitly defined)
    end_date = datetime(2027, 1, 1) # January 1st, 2030 at 00:00:00

    #dates = []
    #current = start_date # Current date is January 1st, 2025 at 00:00:00

    #while current <= end_date:
        #dates.append({'date_key': int(current.strftime('%Y%m%d')), 'date':current, 'year':current.year, 'quarter':(current.month -1) // 3 + 1, 'month':current.month, 'month_name':current.strftime('%B'), 'day_of_month': current.day, 'day_of_week':current.isoweekday(), 'day_name': current.strftime('%A'), 'week_of_year':current.isocalendar()[1], 'is_weekend':current.isoweekday() >= 6})
        #current += timedelta(days=1)
    
    dim_date_df = spark.sql("SELECT explode(sequence(to_date('2026-01-01'), to_date('2027-01-01'), interval 1 day)) AS date").select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), F.col("date"), F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month"), F.date_format("date", "MMMM").alias("month_name"), F.dayofmonth("date").alias("day_of_month"), F.dayofweek("date").alias("day_of_week"), F.date_format("date", "EEEE").alias("day_name"), F.weekofyear("date").alias("week_of_year"), F.dayofweek("date").isin([1, 7]).alias("is_weekend"))

    dim_date_df.coalesce(1).write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_date")


    # Create fact_sales table
    print("Creating fact_sales table...")

    fact_sales = spark.sql("SELECT oi.order_id, oi.product_id, o.customer_id, o.order_date, p.cost, oi.unit_price, oi.quantity, oi.subtotal, ((oi.quantity*oi.unit_price)-(oi.quantity*p.cost)) as profit FROM silver_order_items oi JOIN silver_orders o ON oi.order_id = o.order_id JOIN silver_products p ON oi.product_id = p.product_id WHERE o.status = 'processing' ").withColumn("sales_key", F.monotonically_increasing_id() + 1).select("sales_key", "order_id","product_id", "customer_id", "order_date", "cost", "unit_price", "quantity", "subtotal", "profit")

    fact_sales.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/fact_sales")


    # Create dim_customer dimension table
    print("Creating dim_customer table...")
    
    dim_customer = spark.sql("SELECT customer_id , CONCAT(first_name, ' ', last_name) as full_name, email, phone, address, city, state FROM silver_customers").withColumn("customer_key", F.monotonically_increasing_id() +1).select("customer_key", "full_name", "email", "phone", "address", "city", "state")

    dim_customer.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_customer")


    # Create dim_products
    print("Creating dim_product table...")
    query_product = """SELECT product_id as product_key, product, category, supplier, price, cost FROM silver_products"""

    dim_product = spark.sql(query_product)

    dim_product.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_product")


    # Create dim_locations 
    print("Creating dim_location table...")
    dim_location = spark.sql("SELECT warehouse_location FROM silver_inventory").withColumn("location_id", F.monotonically_increasing_id() + 1).select("location_id", "warehouse_location")

    dim_location.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_location")


    # Create fact_order_status
    print("Creating fact_order_status table...")

    fact_order_status = spark.sql("SELECT order_id, customer_id, MAX(CASE WHEN status = 'processing' THEN updated_status_at END) as order_date, MAX(CASE WHEN status  = 'shipped' THEN updated_status_at END) as shipped_date, MAX(CASE WHEN status = 'delivered' THEN updated_status_at END) as delivered_date FROM silver_orders_undeduplicated GROUP BY order_id, customer_id ORDER BY order_id").withColumn("order_key", F.monotonically_increasing_id() + 1).withColumn("days_to_ship", F.datediff(F.col("shipped_date"), F.col("order_date"))).withColumn("days_to_deliver", F.datediff(F.col("delivered_date"), F.col("order_date"))).select("order_key", "order_id", "customer_id", "order_date", "shipped_date", "delivered_date", "days_to_ship", "days_to_deliver")

    fact_order_status.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/fact_order_status")


    # Create fact_inventory
    print("Creating fact_inventory_snapshot table...")

    fact_inventory_snapshot = spark.sql("SELECT inventory_id, product_id, warehouse_location, quantity, reorder_level, last_restocked FROM silver_inventory ORDER BY inventory_id").withColumn("inventory_key", F.monotonically_increasing_id() + 1).withColumn("snapshot_timestamp", F.date_trunc("second", F.current_timestamp())).select("inventory_key","inventory_id","product_id","warehouse_location","quantity","reorder_level","last_restocked")

    fact_inventory_snapshot.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/fact_inventory_snapshot")


    print("")
    print("="*60)
    print("Gold layer processing complete")
    print("="*60, '\n')


    # Verifying data
    print('\n') 
    print("="*60)
    print("Verifying Gold Layer Data")
    print("="*60, '\n')

    print("Fact_sales table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/fact_sales").show(10, truncate = False)

    print("Dim_customer table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_customer").show(10, truncate = False)
    
    print("Dim_product table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_product").show(10, truncate = False)
    
    print("Dim_location table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_location").show(10, truncate = False)

    print("Dim_date table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/dim_date").show(10, truncate = False)
    
    print("Fact_order_status_table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/fact_order_status").show(50, truncate = False)

    print("Fact_inventory_snapshot table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/gold/fact_inventory_snapshot").show(10, truncate = False)

    spark.stop()