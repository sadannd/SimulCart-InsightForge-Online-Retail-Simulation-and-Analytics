from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, collect_list, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingCSVWriter") \
    .getOrCreate()

# Define the schema for the JSON data
schema = StructType([
    StructField("Interaction_ID", IntegerType()),
    StructField("Timestamp", TimestampType()),
    StructField("User_ID", IntegerType()),
    StructField("Interaction_Type", StringType()),
    StructField("Product_ID", IntegerType()),
    StructField("Product_Name", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("Payment_Method", StringType()),
    StructField("Search_Query", StringType())
])

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "User_Interactions") \
    .load()

# Parse JSON data and select relevant columns
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Group by window and write to CSV
query = df_parsed \
    .withWatermark("Timestamp", "1 minute") \
    .groupBy(window("Timestamp", "1 minute"), "Interaction_ID") \
    .agg(
        collect_list("Timestamp").alias("Timestamp"),
        collect_list("User_ID").alias("User_ID"),
        collect_list("Interaction_Type").alias("Interaction_Type"),
        collect_list("Product_ID").alias("Product_ID"),
        collect_list("Product_Name").alias("Product_Name"),
        collect_list("Quantity").alias("Quantity"),
        collect_list("Payment_Method").alias("Payment_Method"),
        collect_list("Search_Query").alias("Search_Query")
    ) \
    .withColumn("Timestamp", concat_ws(",", col("Timestamp"))) \
    .withColumn("User_ID", concat_ws(",", col("User_ID"))) \
    .withColumn("Interaction_Type", concat_ws(",", col("Interaction_Type"))) \
    .withColumn("Product_ID", concat_ws(",", col("Product_ID"))) \
    .withColumn("Product_Name", concat_ws(",", col("Product_Name"))) \
    .withColumn("Quantity", concat_ws(",", col("Quantity"))) \
    .withColumn("Payment_Method", concat_ws(",", col("Payment_Method"))) \
    .withColumn("Search_Query", concat_ws(",", col("Search_Query"))) \
    .withColumn("Window_Start", col("window.start").cast(StringType())) \
    .withColumn("Window_End", col("window.end").cast(StringType())) \
    .select("Window_Start", "Window_End", "Interaction_ID", "Timestamp", "User_ID", "Interaction_Type", "Product_ID", "Product_Name", "Quantity", "Payment_Method", "Search_Query") \
    .repartition(2)  # Set the number of partitions to 2
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .option("path", "s3://bigdataraj/data/") \  # Specify your S3 path here
    .option("truncate", "false") \
    .start()

# Await termination of the query
query.awaitTermination()

