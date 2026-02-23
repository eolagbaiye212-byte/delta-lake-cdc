# This script executes the bronze-silver-gold architecture for the retail data

from spark_jobs import bronze_layer
from spark_jobs import silver_layer
from spark_jobs import gold_layer
import log_progress

# Define file path for saving etl logging data
log_file = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Data Lake with CDC Project/etl_log.txt"

# Create a function that executes the bronze-silver-gold artichecture for the data
def main():
    log_progress.log_progress(log_file, "ETL Process Started")

    bronze_layer.bronze_layer()
    log_progress.log_progress(log_file, "Bronze layer complete")

    silver_layer.silver_layer()
    log_progress.log_progress(log_file, "Silver layer complete")

    gold_layer.gold_layer()
    log_progress.log_progress(log_file, "Gold layer complete")

if __name__ == "__main__":
    main()