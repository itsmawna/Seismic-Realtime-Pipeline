from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import time

# Set Spark local IP
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Minimal Spark session
spark = SparkSession.builder \
    .appName("SeismicProcessor") \
    .master("local[1]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Full schema
schema = StructType([
    StructField("action", StringType(), True),
    StructField("mag", DoubleType(), True),
    StructField("flynn_region", StringType(), True),
    StructField("time", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("magtype", StringType(), True),
    StructField("evtype", StringType(), True)
])

print("=" * 70)
print("SEISMIC REAL-TIME PIPELINE")
print("=" * 70)
print("Connecting to Kafka (localhost:9092)...")
print("=" * 70)
print()

# Keep track of already displayed events
processed_events = set()
event_count = 0

while True:
    try:
        # Read all current messages from Kafka
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "RawSeismicData") \
            .option("startingOffsets", "earliest") \
            .load()
        
        if df.count() > 0:
            # Parse JSON
            json_df = df.selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), schema).alias("data")) \
                .select("data.*") \
                .filter(col("mag") >= 2.0)
            
            # Collect events
            events = json_df.collect()
            
            # Display only new events
            for event in events:
                # Unique ID based on time + coordinates
                event_id = f"{event.time}_{event.lat}_{event.lon}"
                
                if event_id not in processed_events:
                    processed_events.add(event_id)
                    event_count += 1
                    
                    print("=" * 70)
                    print(f"EVENT #{event_count}")
                    print("=" * 70)
                    print(f"  Magnitude : {event.mag}")
                    print(f"  Region    : {event.flynn_region}")
                    print(f"  Time      : {event.time}")
                    print(f"  Latitude  : {event.lat}")
                    print(f"  Longitude : {event.lon}")
                    print(f"  Depth     : {event.depth} km")
                    if event.magtype:
                        print(f"  Magnitude Type : {event.magtype}")
                    if event.action:
                        print(f"  Action         : {event.action}")
                    print("=" * 70)
                    print()
        
        # Wait 5 seconds before next read
        time.sleep(5)
        
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("PIPELINE STOPPED")
        print("=" * 70)
        print(f"Total unique events processed: {event_count}")
        print("=" * 70)
        break
        
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)

spark.stop()
print("Spark stopped cleanly")
