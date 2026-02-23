# This script creates the bronze layer of the data by use spark to extract data from Kafka the source and loading it into the bronze layer (delta lake).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta import *
import os
import sys

def bronze_layer():
    os.environ["HADOOP_HOME"] = "C:\\hadoop" # Path to the folder containing the bin folder
    os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

    # Begin Spark Session with Delta Lake
    spark = SparkSession.builder.appName("Bronze Layer - CDC Ingestion").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    print("="*60)
    print("BRONZE LAYER: Ingesting CDC Event from Kafka")
    print("="*60)

    # Kafka Configuration
    kafka_bootstrap_servers = "localhost:9092"
    topics = ["retaildb.public.customers", "retaildb.public.products", "retaildb.public.inventory", "retaildb.public.orders", "retaildb.public.order_items"]

    for topic in topics:
        table_name = topic.split(".")[-1]  # Extract table name from topic
        print(f"Ingesting data from topic: {topic} into bronze layer as table '{table_name}'")

        # Read from Kafka
        df = spark.read.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", topic).option("startingOffsets", "earliest").load()

        # Transform: Keep raw JSON & metadata
        bronze_df = df.select(col("value").cast("string").alias("raw_json"), col("topic"), col("partition"), col("offset"), col("timestamp").alias("kafka_timestamp"), current_timestamp().alias("ingestion_timestamp"))

        # Define putput path to wrote data to
        output_path = f"C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/delta_lake/bronze/{table_name}"

        # Check if Delta table exists
        try: # If table exists, merge new records to that table
            delta_table = DeltaTable.forPath(spark, output_path)

            delta_table.alias("target").merge(bronze_df.alias("source"), """target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset""").whenNotMatchedInsertAll().execute()

            print(f"Merged new records into {output_path}")

        except Exception as e: # If table does not exist, create it
            print(f"Creating new Delta table at {output_path}")
            bronze_df.write.format("delta").mode("append").save(output_path)
            print(f"Written {bronze_df.count()} {table_name} records to Bronze layer")
            print("")

        # Show total record count after processing
        total_count = spark.read.format("delta").load(output_path).count()
        print(f"Total records in {table_name}: {total_count}")

    print("="*60)
    print("Bronze layer processing complete") 
    print("="*60)

    spark.stop()