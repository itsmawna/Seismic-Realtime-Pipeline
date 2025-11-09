from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import time

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Session Spark minimaliste
spark = SparkSession.builder \
    .appName("SeismicProcessor") \
    .master("local[1]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema complet
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
print("🌍 PIPELINE SISMIQUE EN TEMPS RÉEL")
print("=" * 70)
print("🔄 Connexion à Kafka (localhost:9092)...")
print("=" * 70)
print()

# Garde en mémoire les événements déjà affichés
processed_events = set()
event_count = 0

while True:
    try:
        # Lit tous les messages actuels de Kafka
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
            
            # Collecte les événements
            events = json_df.collect()
            
            # Affiche seulement les nouveaux
            for event in events:
                # ID unique basé sur time + coordonnées
                event_id = f"{event.time}_{event.lat}_{event.lon}"
                
                if event_id not in processed_events:
                    processed_events.add(event_id)
                    event_count += 1
                    
                    print(f"{'='*70}")
                    print(f"🆕 ÉVÉNEMENT #{event_count}")
                    print(f"{'='*70}")
                    print(f"  🔴 Magnitude    : {event.mag}")
                    print(f"  📍 Région       : {event.flynn_region}")
                    print(f"  🕐 Temps        : {event.time}")
                    print(f"  🌐 Latitude     : {event.lat}")
                    print(f"  🌐 Longitude    : {event.lon}")
                    print(f"  ⬇️  Profondeur   : {event.depth} km")
                    if event.magtype:
                        print(f"  📊 Type mag     : {event.magtype}")
                    if event.action:
                        print(f"  ⚡ Action       : {event.action}")
                    print(f"{'='*70}")
                    print()
        
        # Attend 5 secondes avant la prochaine lecture
        time.sleep(5)
        
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("🛑 ARRÊT DU PIPELINE")
        print("=" * 70)
        print(f"📊 Total d'événements uniques traités : {event_count}")
        print("=" * 70)
        break
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        time.sleep(5)

spark.stop()
print("✅ Spark arrêté proprement")