"""
Spark Streaming - Reddit CAN 2025
Version corrigée avec sauvegarde MongoDB via pymongo
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
from datetime import datetime
import re

print("="*70)
print("⚡ SPARK STREAMING - REDDIT CAN 2025")
print("="*70)

print("\n🚀 Initialisation de Spark...")

# SparkSession avec SEULEMENT Kafka (pas MongoDB connector)
spark = SparkSession.builder \
    .appName("RedditCANStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session créée!")

# Connexion MongoDB via pymongo
print("\n🔌 Connexion à MongoDB...")
mongo_client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
db = mongo_client['reddit_can']
processed_collection = db['processed_posts']
processed_collection.create_index('id', unique=True)
print("✅ MongoDB connecté!")

# Schema JSON
post_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("url", StringType(), True),
])

# Lire depuis Kafka
print("\n📡 Connexion à Kafka...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "reddit-can-posts") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✅ Connecté à Kafka!")

# Parser JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), post_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# UDF pour nettoyer le texte
@udf(returnType=StringType())
def clean_text(text):
    """Nettoie et normalise le texte"""
    if not text or text == "":
        return ""
    text = str(text).lower()
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#(\w+)', r'\1', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Traitement des données
print("\n🔧 Configuration du traitement...")
processed_df = parsed_df \
    .filter(col("type") == "post") \
    .withColumn("cleaned_title", clean_text(col("title"))) \
    .withColumn("cleaned_body", clean_text(col("selftext"))) \
    .withColumn("combined_text", 
                concat_ws(" ", col("cleaned_title"), col("cleaned_body"))) \
    .withColumn("text_length", length(col("combined_text"))) \
    .withColumn("word_count", size(split(col("combined_text"), " "))) \
    .withColumn("processed_at", current_timestamp()) \
    .filter(col("text_length") > 20)

# Console output (debug)
print("\n📺 Démarrage du stream console...")
query_console = processed_df \
    .select("id", "cleaned_title", "text_length", "word_count", "score") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 3) \
    .trigger(processingTime='30 seconds') \
    .start()

# Fonction pour sauvegarder dans MongoDB
def save_batch_to_mongodb(batch_df, batch_id):
    """Sauvegarde chaque batch dans MongoDB via pymongo"""
    try:
        # Convertir le DataFrame en liste de dictionnaires
        rows = batch_df.collect()
        
        if not rows:
            print(f"⚠️  Batch {batch_id}: Aucune donnée")
            return
        
        documents = []
        for row in rows:
            doc = row.asDict()
            
            # Convertir les timestamps Spark en ISO string
            if 'processed_at' in doc and doc['processed_at']:
                doc['processed_at'] = doc['processed_at'].isoformat()
            if 'kafka_timestamp' in doc and doc['kafka_timestamp']:
                doc['kafka_timestamp'] = doc['kafka_timestamp'].isoformat()
            
            documents.append(doc)
        
        # Insérer dans MongoDB (ignore les doublons)
        inserted_count = 0
        for doc in documents:
            try:
                processed_collection.insert_one(doc)
                inserted_count += 1
            except Exception as e:
                if 'duplicate key' not in str(e).lower():
                    print(f"⚠️  Erreur insertion: {e}")
        
        if inserted_count > 0:
            print(f"✅ Batch {batch_id}: {inserted_count}/{len(documents)} documents insérés dans MongoDB")
        else:
            print(f"⚠️  Batch {batch_id}: {len(documents)} doublons ignorés")
            
    except Exception as e:
        print(f"❌ Erreur batch {batch_id}: {e}")

# Stream MongoDB via foreachBatch
print("\n💾 Démarrage du stream MongoDB...")
query_mongo = processed_df \
    .writeStream \
    .foreachBatch(save_batch_to_mongodb) \
    .trigger(processingTime='30 seconds') \
    .start()

print("\n" + "="*70)
print("🚀 SPARK STREAMING DÉMARRÉ AVEC SUCCÈS!")
print("="*70)
print("\n📊 Configuration:")
print("   - Lecture depuis: Kafka (topic: reddit-can-posts)")
print("   - Écriture vers: MongoDB (collection: processed_posts)")
print("   - Intervalle de traitement: 30 secondes")
print("   - Méthode: foreachBatch avec pymongo")
print("\n⏳ En attente de nouveaux messages...\n")

# Attendre les deux queries
try:
    query_mongo.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Arrêt demandé...")
    query_console.stop()
    query_mongo.stop()
    spark.stop()
    print("✅ Spark arrêté proprement")