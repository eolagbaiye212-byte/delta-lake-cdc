# This script is engages the silver layer of the Bronze-Silver-Gold architecture - ingests the bronze layer and performs transformations on the data

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import sys

def silver_layer():
    os.environ["HADOOP_HOME"] = "C:\\hadoop" # Path to the folder containing the bin folder
    os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

    # Create Spark session with Delta Lake
    spark = SparkSession.builder.appName("Silver Layer - Data Cleaning").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("="*60)
    print("SILVER LAYER: Cleaning Raw Data")
    print("="*60)

    # Read Bronze data and assign data from each table to a dataframe
    bronze_df_customer = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/bronze/customers")

    bronze_df_inventory = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/bronze/inventory")

    bronze_df_order_items = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/bronze/order_items")

    bronze_df_orders = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/bronze/orders")

    bronze_df_products = spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/bronze/products")

    # Create temporary views of each dataframe 
    bronze_df_customer.createOrReplaceTempView("bronze_customers")
    print(f"Bronze_df_customer count: {bronze_df_customer.count()} \n")

    bronze_df_products.createOrReplaceTempView("bronze_products")
    print(f"Bronze_df_products count: {bronze_df_products.count()} \n")

    bronze_df_inventory.createOrReplaceTempView("bronze_inventory")
    print(f"Bronze_df_inventory count: {bronze_df_inventory.count()} \n")

    bronze_df_orders.createOrReplaceTempView("bronze_orders")
    print(f"Bronze_df_orders count: {bronze_df_orders.count()} \n")

    bronze_df_order_items.createOrReplaceTempView("bronze_order_items")
    print(f"Bronze_df_order_items count: {bronze_df_order_items.count()} \n")

    # Clean customer data using bronze_customers temporary view
    print("Cleaning customer data...")
    query_customer = """WITH parsed AS (SELECT CAST(get_json_object(raw_json, '$.payload.after.customer_id') AS INT) as customer_id, get_json_object(raw_json, '$.payload.after.first_name') as first_name, get_json_object(raw_json, '$.payload.after.last_name') as last_name, get_json_object(raw_json, '$.payload.after.email') as email, CAST(get_json_object(raw_json, '$.payload.after.phone') AS STRING) as phone, get_json_object(raw_json, '$.payload.after.address') as address, get_json_object(raw_json, '$.payload.after.city') as city, get_json_object(raw_json, '$.payload.after.state') as state, CAST(get_json_object(raw_json, '$.payload.after.zipcode') AS STRING) as zipcode, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.created_at') AS BIGINT))) as created_at, get_json_object(raw_json, '$.payload.op') as operation, kafka_timestamp FROM bronze_customers WHERE get_json_object(raw_json, '$.payload.op') IN ('c', 'u', 'r')), deduplicated AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY kafka_timestamp DESC) as rn FROM parsed), cleaned1 AS (SELECT customer_id, TRIM(first_name) as first_name, TRIM(last_name) as last_name, LOWER(TRIM(email)) as email, CAST(split_part(phone,'x',1) AS STRING) as phone, regexp_replace(address, '\n', ' ') as address ,  UPPER(TRIM(city)) as city, UPPER(TRIM(state)) as state, zipcode, operation FROM deduplicated WHERE rn = 1), cleaned2 AS (SELECT customer_id, first_name, last_name, email, right(regexp_replace(phone,'[^0-9]',''), 10) as phone, address, city, state, zipcode, operation FROM cleaned1) SELECT * FROM cleaned2"""

    silver_customer = spark.sql(query_customer)
    silver_customer.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/customers") # Save transformed data

    print(f"Processed {silver_customer.count()} customer records to Silver layer \n")


    # Clean products data using bronze_products temporary view
    print("Cleaning product data...")
    query_products = """WITH parsed AS (SELECT CAST(get_json_object(raw_json, '$.payload.after.product_id') AS INT) as product_id, get_json_object(raw_json, '$.payload.after.product') as product, get_json_object(raw_json, '$.payload.after.category') as category, ROUND(CAST(conv(hex(unbase64(get_json_object(raw_json, '$.payload.after.price'))),16,10) AS DECIMAL(12,2))/100, 2) as price, ROUND(CAST(conv(hex(unbase64(get_json_object(raw_json, '$.payload.after.cost'))),16,10) AS DECIMAL(12,2))/100, 2) as cost, get_json_object(raw_json, '$.payload.after.supplier') as supplier, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.created_at') AS BIGINT))) as created_at, get_json_object(raw_json, '$.payload.op') as operation, kafka_timestamp FROM bronze_products WHERE get_json_object(raw_json, '$.payload.op') IN ('c', 'u', 'r')), deduplicated AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY kafka_timestamp DESC) as rn FROM parsed), cleaned AS (SELECT product_id, product, category, price, cost, supplier, created_at, operation FROM deduplicated WHERE rn = 1) SELECT * FROM cleaned"""

    silver_products = spark.sql(query_products)
    silver_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/products")

    print(f"Processed {silver_products.count()} products records to Silver layer \n")


    # Clean inventory data using bronze_inventory temporary view
    print("Cleaning inventory data...")
    query_inventory = """WITH parsed AS (SELECT CAST(get_json_object(raw_json, '$.payload.after.inventory_id') AS INT) as inventory_id, CAST(get_json_object(raw_json, '$.payload.after.product_id') AS INT) as product_id, get_json_object(raw_json, '$.payload.after.warehouse_location') as warehouse_location, CAST(get_json_object(raw_json, '$.payload.after.quantity') as INT) as quantity, CAST(get_json_object(raw_json, '$.payload.after.reorder_level') AS INT) as reorder_level, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.last_restocked') AS BIGINT))) as last_restocked, get_json_object(raw_json, '$.payload.op') as operation, kafka_timestamp FROM bronze_inventory WHERE get_json_object(raw_json, '$.payload.op') IN ('c', 'u', 'r')), deduplicated AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY inventory_id ORDER BY kafka_timestamp DESC) as rn FROM parsed),cleaned AS (SELECT inventory_id, product_id, warehouse_location, quantity, reorder_level, last_restocked, operation FROM deduplicated WHERE rn = 1) SELECT * FROM cleaned"""

    silver_inventory = spark.sql(query_inventory)
    silver_inventory.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/inventory")

    print(f"Processed {silver_inventory.count()} inventory records to Silver layer \n")


    # Clean orders data using bronze_orders temporary view
    print("Cleaning orders data...")
    query_orders = """WITH parsed AS (SELECT CAST(get_json_object(raw_json, '$.payload.after.order_id') AS INT) as order_id, CAST(get_json_object(raw_json, '$.payload.after.customer_id') AS INT) as customer_id, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.order_date') AS BIGINT))) as order_date, ROUND(CAST(conv(hex(unbase64(get_json_object(raw_json, '$.payload.after.total_amount'))),16,10) AS DECIMAL(12,2))/100, 2) as total_amount, get_json_object(raw_json, '$.payload.after.status') as status, get_json_object(raw_json, '$.payload.after.shipping_address') as shipping_address, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.updated_at') AS BIGINT))) as updated_status_at, kafka_timestamp, get_json_object(raw_json, '$.payload.op') as operation FROM bronze_orders WHERE get_json_object(raw_json, '$.payload.op') IN ('c', 'u', 'r')), deduplicated AS (SELECT order_id, customer_id, order_date, total_amount, status, shipping_address, updated_status_at, operation, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY kafka_timestamp DESC, updated_status_at DESC) as rn FROM parsed), 
    latest_per_status AS (SELECT order_id, customer_id, order_date, total_amount, status, shipping_address, updated_status_at, operation FROM deduplicated WHERE rn = 1), cleaned AS (SELECT order_id, customer_id, order_date, total_amount, status, regexp_replace(shipping_address, '\n', ' ') as shipping_address, updated_status_at, operation FROM latest_per_status) SELECT * FROM cleaned"""

    silver_orders = spark.sql(query_orders)
    silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/orders")

    print(f"Processed {silver_orders.count()} orders records to Silver layer \n")


    # Clean order_items data using bronze_order_items temporary view
    print("Cleaning order_items data...")
    query_order_items = """WITH parsed AS (SELECT CAST(get_json_object(raw_json, '$.payload.after.order_item_id') AS INT) as order_item_id, CAST(get_json_object(raw_json, '$.payload.after.order_id') AS INT) as order_id, CAST(get_json_object(raw_json, '$.payload.after.product_id') AS INT) as product_id, CAST(get_json_object(raw_json, '$.payload.after.quantity') AS INT) as quantity, ROUND(CAST(conv(hex(unbase64(get_json_object(raw_json, '$.payload.after.unit_price'))),16,10) AS DECIMAL(12,2))/100, 2) as unit_price, ROUND(CAST(conv(hex(unbase64(get_json_object(raw_json, '$.payload.after.subtotal'))),16,10) AS DECIMAL(12,2))/100, 2) as subtotal, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.created_at') AS BIGINT))) as created_at, get_json_object(raw_json, '$.payload.op') as operation, kafka_timestamp FROM bronze_order_items WHERE get_json_object(raw_json, '$.payload.op') IN ('c', 'u', 'r')), deduplicated AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY kafka_timestamp DESC) as rn FROM parsed), cleaned AS (SELECT order_item_id, order_id, product_id, quantity, unit_price, subtotal, created_at, operation FROM deduplicated WHERE rn = 1) SELECT * FROM cleaned"""

    silver_order_items = spark.sql(query_order_items)
    silver_order_items.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/order_items")

    print(f"Processed {silver_order_items.count()} order_items records to Silver layer \n")
    print("")
    print("="*60)
    print("Silver layer processing complete")
    print("="*60, '\n')

    # Special table in silver layer that contains un-deduplicated data from the bronze_orders table; feeds fact_order_status table so that update times for each status for a particular order are provided.
    print("Creating un-deduplicated orders table....")
    query_orders_special = """WITH parsed AS (SELECT CAST(get_json_object(raw_json, '$.payload.after.order_id') AS INT) as order_id, CAST(get_json_object(raw_json, '$.payload.after.customer_id') AS INT) as customer_id, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.order_date') AS BIGINT))) as order_date, ROUND(CAST(conv(hex(unbase64(get_json_object(raw_json, '$.payload.after.total_amount'))),16,10) AS DECIMAL(12,2))/100, 2) as total_amount, get_json_object(raw_json, '$.payload.after.status') as status, get_json_object(raw_json, '$.payload.after.shipping_address') as shipping_address, date_trunc('second', timestamp_micros(CAST(get_json_object(raw_json, '$.payload.after.updated_at') AS BIGINT))) as updated_status_at, get_json_object(raw_json, '$.payload.op') as operation FROM bronze_orders WHERE get_json_object(raw_json, '$.payload.op') IN ('c', 'u', 'r')), cleaned AS (SELECT order_id, customer_id, order_date, total_amount, status, regexp_replace(shipping_address, '\n', ' ') as shipping_address, updated_status_at, operation FROM parsed) SELECT * FROM cleaned ORDER BY order_id"""

    silver_orders_undeduplicated = spark.sql(query_orders_special)
    silver_orders_undeduplicated.write.format("delta").mode("overwrite").save("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/orders_undeduplicated")

    spark.read.format("delta").load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/orders_undeduplicated").show(10, truncate = False)


    # Verifying data
    print("\n") 
    print("="*60) 
    print("Verifying Silver Layer Data")
    print("="*60, '\n')

    print("Customers table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/customers").show(10, truncate = False)

    print("Products table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/products").show(10, truncate = False)
    
    print("Inventory table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/inventory").show(10, truncate = False)
    
    print("Orders table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/orders").orderBy("order_id").show(20, truncate = False)
    
    print("Order_Items table")
    spark.read.format('delta').load("C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/silver/order_items").orderBy("order_id").show(20 , truncate = False)

    spark.stop()