from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, sum as _sum, to_json, struct, coalesce, lit, count, desc, row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from prometheus_client import Gauge, start_http_server
import uuid

start_http_server(8080)

spark = SparkSession.builder \
    .appName("TrafficMonitoring") \
    .config("spark.sql.streaming.checkpointLocation", "file:///tmp/spark-checkpoints") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("congestion_level", StringType(), True)
])

raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .option("startingOffsets", "latest") \
    .load()

df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df_valid = df.filter(
    col("sensor_id").isNotNull() & col("timestamp").isNotNull() & 
    (col("vehicle_count") >= 0) & (col("average_speed") > 0)
).dropDuplicates(["sensor_id", "timestamp"]) \
 .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

top_congested_gauge = Gauge(
    "top_congested_sensors",
    "Top congested sensors based on vehicle count",
    ["sensor_id", "congestion_level"]
)

avg_speed_gauge = Gauge(
    "sensor_avg_speed",
    "Average vehicle speed per sensor",
    ["sensor_id"]
)

vehicle_count_gauge = Gauge(
    "sensor_vehicle_count",
    "Average vehicle count per sensor",
    ["sensor_id"]
)

high_congestion_gauge = Gauge(
    "sensor_high_congestion_events",
    "Number of high congestion events per sensor",
    ["sensor_id"]
)

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"=== Batch {batch_id}: No data received ===")
        return

    print(f"=== Batch {batch_id}: Processing data for Prometheus ===")

    # Top congested sensors
    aggregated_df = batch_df.groupBy(
        window(col("event_time"), "1 minute"), col("sensor_id"), col("congestion_level")
    ).agg(count("*").alias("event_count"))

    w = Window.partitionBy("congestion_level", "window").orderBy(desc("event_count"))

    top_congested = aggregated_df.withColumn("rn", row_number().over(w)).filter(col("rn") <= 3)

    for row in top_congested.collect():
        sensor_id = row['sensor_id']
        congestion_level = row['congestion_level']
        count_value = row['event_count']
        print(f"Prometheus - Top: {sensor_id}, Count: {count_value}, Level: {congestion_level}")
        top_congested_gauge.labels(sensor_id=sensor_id, congestion_level=congestion_level).set(count_value)

    # Avg speed and vehicle count
    avg_metrics = batch_df.groupBy("sensor_id").agg(
        avg("average_speed").alias("avg_speed"),
        avg("vehicle_count").alias("avg_vehicle_count")
    ).collect()

    for row in avg_metrics:
        sensor_id = row["sensor_id"]
        avg_speed = row["avg_speed"] or 0
        avg_count = row["avg_vehicle_count"] or 0
        avg_speed_gauge.labels(sensor_id=sensor_id).set(avg_speed)
        vehicle_count_gauge.labels(sensor_id=sensor_id).set(avg_count)

    # High congestion events
    high_congestion = batch_df.filter(col("congestion_level") == "HIGH") \
        .groupBy("sensor_id").count().withColumnRenamed("count", "high_congestion_count") \
        .collect()

    for row in high_congestion:
        sensor_id = row["sensor_id"]
        count_val = row["high_congestion_count"]
        high_congestion_gauge.labels(sensor_id=sensor_id).set(count_val)

# 1. Volume (5-minute window)
volume_df = df_valid.groupBy(window("event_time", "5 minutes"), "sensor_id") \
    .agg(_sum("vehicle_count").alias("volume_5min")) \
    .withColumn("value", to_json(struct(
        "sensor_id", 
        col("window.start").alias("window_start"), 
        col("window.end").alias("window_end"), 
        "volume_5min"
    )))

# 2. Average Speed (10-minute window)
avg_speed_df = df_valid.groupBy(window("event_time", "10 minutes"), "sensor_id") \
    .agg(avg("average_speed").alias("avg_speed")) \
    .withColumn("value", to_json(struct(
        "sensor_id", 
        col("window.start").alias("window_start"), 
        col("window.end").alias("window_end"), 
        "avg_speed"
    )))

# 3. Busiest Sensors (30-minute window)
busiest_df = df_valid.groupBy(window("event_time", "30 minutes"), "sensor_id") \
    .agg(_sum("vehicle_count").alias("volume_30min")) \
    .withColumn("value", to_json(struct(
        "sensor_id", 
        col("window.start").alias("window_start"), 
        col("window.end").alias("window_end"), 
        "volume_30min"
    )))

# 4. Congestion Hotspots (persistent HIGH congestion)
window_spec = Window.partitionBy("sensor_id").orderBy("window.start")

congestion_df = df_valid.groupBy(
    window("event_time", "5 minutes"), "sensor_id"
).agg(F.max("congestion_level").alias("congestion_level"))

congestion_hotspots = congestion_df.withColumn("prev_1", F.lag("congestion_level", 1).over(window_spec)) \
    .withColumn("prev_2", F.lag("congestion_level", 2).over(window_spec)) \
    .filter((F.col("congestion_level") == "HIGH") & 
            (F.col("prev_1") == "HIGH") & 
            (F.col("prev_2") == "HIGH")) \
    .withColumn("value", to_json(struct(
        "sensor_id",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "congestion_level"
    )))

# 5. Sudden Speed Drops (2-minute window)
speed_df = df_valid.groupBy(window("event_time", "2 minutes"), "sensor_id") \
    .agg(F.avg("average_speed").alias("avg_speed"))

speed_drop_df = speed_df.withColumn("prev_speed", F.lag("avg_speed", 1).over(window_spec))

speed_drop_df = speed_drop_df.withColumn("speed_drop", 
    (F.coalesce(F.col("prev_speed"), lit(0.001)) - F.col("avg_speed")) / 
    F.coalesce(F.col("prev_speed"), lit(0.001))
)

sudden_drops = speed_drop_df.filter(F.col("speed_drop") > 0.5) \
    .withColumn("value", to_json(struct(
        "sensor_id",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_speed",
        "prev_speed",
        "speed_drop"
    )))

def write_to_kafka(df, topic, checkpoint_path):
    return df.selectExpr("CAST(sensor_id AS STRING) as key", "value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("update") \
        .start()

query1 = write_to_kafka(volume_df, "traffic_volume", "file:///tmp/kafka-checkpoints/traffic_volume")
query2 = write_to_kafka(avg_speed_df, "traffic_speed", "file:///tmp/kafka-checkpoints/traffic_speed")
query3 = write_to_kafka(busiest_df, "traffic_busiest", "file:///tmp/kafka-checkpoints/traffic_busiest")
query4 = write_to_kafka(congestion_hotspots, "traffic_congestion", "file:///tmp/kafka-checkpoints/traffic_congestion")
query5 = write_to_kafka(sudden_drops, "traffic_speed_drops", "file:///tmp/kafka-checkpoints/traffic_speed_drops")

metrics_query = df_valid.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()
