from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import yaml
import db_utils

# Load Redshift configuration
with open("config/db_config.yaml", "r") as f:
    db_config = yaml.safe_load(f)

# Kafka topic and broker configuration
KAFKA_TOPIC = "hospital-events"
KAFKA_BROKER = "broker:9092"

# Define schema for Kafka data
hospital_schema = StructType() \
    .add("hospital_id", StringType()) \
    .add("hospital_name", StringType()) \
    .add("location", StringType()) \
    .add("services", ArrayType(StringType())) \
    .add("vacancies", IntegerType()) \
    .add("type", StringType())  # e.g., General, Specialist, etc.

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KafkaStreamHospitalAnalyser") \
        .getOrCreate()

    # Read data from Kafka topic
    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Convert Kafka value from bytes to string, then parse JSON
    df_value = df_kafka.selectExpr("CAST(value AS STRING)")
    df_parsed = df_value.withColumn("data", from_json(col("value"), hospital_schema)) \
        .select("data.*")

    # Process data: Filter hospitals with vacancies > 0
    df_filtered = df_parsed.filter(col("vacancies") > 0)

    # Expand services list for easier searching (one row per service)
    df_services = df_filtered.withColumn("service", explode(col("services")))

    # Write to Redshift
    def write_to_redshift(batch_df, batch_id):
        db_utils.save_to_redshift(batch_df, db_config, "hospital_data")

    # Start the Spark streaming query and write to Redshift in micro-batches
    df_services.writeStream \
        .foreachBatch(write_to_redshift) \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()
