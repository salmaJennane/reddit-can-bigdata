from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Créer SparkSession
logger.info("🚀 Creating Spark Session...")
spark = SparkSession.builder \
    .appName("RedditCANStreamProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/reddit_can.processed_data?authSource=admin") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("✅ Spark Session created!")

# Schema pour les posts Reddit
post_schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("author", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("upvote_ratio", DoubleType(), True),
    StructField("subreddit", StringType(), True),
    StructField("url", StringType(), True),
    StructField("flair", StringType(), True),
    StructField("permalink", StringType(), True)
])

# Schema pour les commentaires
comment_schema = StructType([
    StructField("id", StringType(), True),
    StructField("body", StringType(), True),
    StructField("author", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("score", IntegerType(), True),
    StructField("parent_id", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("permalink", StringType(), True)
])

# Lire depuis Kafka - POSTS
logger.info("📡 Connecting to Kafka for POSTS...")
posts_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "reddit-posts") \
    .option("startingOffsets", "earliest") \
    .load()

# Parser le JSON des posts
parsed_posts = posts_df.select(
    from_json(col("value").cast("string"), post_schema).alias("data")
).select("data.*")

# UDF pour nettoyer le texte
@udf(returnType=StringType())
def clean_text(text):
    if text is None or text == "":
        return ""
    import re
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)  # URLs
    text = re.sub(r'@\w+', '', text)  # Mentions
    text = re.sub(r'#(\w+)', r'\1', text)  # Hashtags (garder le mot)
    text = re.sub(r'[^\w\s]', ' ', text)  # Ponctuation
    text = re.sub(r'\s+', ' ', text).strip()  # Espaces multiples
    return text

# Traitement des POSTS
processed_posts = parsed_posts \
    .withColumn("cleaned_title", clean_text(col("title"))) \
    .withColumn("cleaned_text", clean_text(col("selftext"))) \
    .withColumn("combined_text", 
                when(col("cleaned_text") != "", 
                     concat_ws(" ", col("cleaned_title"), col("cleaned_text")))
                .otherwise(col("cleaned_title"))) \
    .withColumn("text_length", length(col("combined_text"))) \
    .withColumn("word_count", size(split(col("combined_text"), " "))) \
    .withColumn("has_text", when(col("text_length") > 5, True).otherwise(False)) \
    .withColumn("engagement_score", col("score") + col("num_comments")) \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("created_date", from_unixtime(col("created_utc"))) \
    .withColumn("data_type", lit("post"))

# Lire depuis Kafka - COMMENTS
logger.info("📡 Connecting to Kafka for COMMENTS...")
comments_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "reddit-comments") \
    .option("startingOffsets", "earliest") \
    .load()

# Parser le JSON des commentaires
parsed_comments = comments_df.select(
    from_json(col("value").cast("string"), comment_schema).alias("data")
).select("data.*")

# Traitement des COMMENTS
processed_comments = parsed_comments \
    .withColumn("cleaned_body", clean_text(col("body"))) \
    .withColumn("text_length", length(col("cleaned_body"))) \
    .withColumn("word_count", size(split(col("cleaned_body"), " "))) \
    .withColumn("has_text", when(col("text_length") > 5, True).otherwise(False)) \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("created_date", from_unixtime(col("created_utc"))) \
    .withColumn("data_type", lit("comment"))

# Écrire les POSTS dans MongoDB
logger.info("💾 Writing POSTS to MongoDB...")
query_posts_mongo = processed_posts \
    .writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-posts") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/reddit_can.processed_posts?authSource=admin") \
    .start()

# Écrire les COMMENTS dans MongoDB
logger.info("💾 Writing COMMENTS to MongoDB...")
query_comments_mongo = processed_comments \
    .writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-comments") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/reddit_can.processed_comments?authSource=admin") \
    .start()

# Afficher dans la console (debug)
query_console_posts = processed_posts \
    .select("id", "cleaned_title", "text_length", "word_count", "engagement_score") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .start()

query_console_comments = processed_comments \
    .select("id", "cleaned_body", "text_length", "word_count") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .start()

logger.info("🚀 Spark Streaming started! Processing data in real-time...")

# Attendre la fin
query_posts_mongo.awaitTermination()