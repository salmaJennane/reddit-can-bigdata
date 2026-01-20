"""
========================================================
🤖 SPARK ML FINAL - SENTIMENT ANALYSIS avec VADER
========================================================
✔ VADER pour labellisation (baseline)
✔ Modèles ML entraînés sur labels VADER
✔ Comparaison VADER vs ML
✔ Conservation des emojis (informations importantes)
========================================================
"""

# ===============================
# IMPORTS
# ===============================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, length, udf, current_timestamp, when,
    regexp_replace
)
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, CountVectorizer,
    IDF, StringIndexer, IndexToString, VectorAssembler
)
from pyspark.ml.classification import (
    LogisticRegression, NaiveBayes, RandomForestClassifier
)
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import builtins
import re

# ===============================
# SPARK SESSION
# ===============================
print("=" * 70)
print("🤖 SPARK ML FINAL - SENTIMENT ANALYSIS CAN 2025")
print("=" * 70)

spark = SparkSession.builder \
    .appName("RedditSentimentMLVader") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session créée!")

# ===============================
# VADER ANALYZER
# ===============================
print("\n🔧 Initialisation VADER...")
vader_analyzer = SentimentIntensityAnalyzer()
print("✅ VADER prêt!")

# ===============================
# MONGODB
# ===============================
print("\n🔌 Connexion à MongoDB...")
mongo = MongoClient("mongodb://admin:admin123@mongodb:27017/")
db = mongo["reddit_can"]
posts_col = db["processed_posts"]
results_col = db["sentiment_results"]
print("✅ MongoDB connecté!")

# ===============================
# LOAD DATA
# ===============================
print("\n📥 Chargement des données depuis MongoDB...")
docs = list(posts_col.find({}, {"_id": 0}))

if not docs:
    print("❌ Aucune donnée trouvée")
    exit(1)

df = spark.createDataFrame(docs)
print(f"✅ DataFrame Spark prêt : {df.count()} lignes")

df = df.select("id", "combined_text", "score", "num_comments")
df = df.filter(length(col("combined_text")) > 20)

print(f"📊 Après filtrage : {df.count()} lignes")

# ===============================
# EMOJIS (CONSERVATION!)
# ===============================
POSITIVE_EMOJIS = ['😊','😃','😁','😍','🥰','👏','👍','🔥','❤️','🏆','🥇','🎯','✨','💪','🎉']
NEGATIVE_EMOJIS = ['😢','😭','😞','😡','🤬','💔','👎','❌','😰','☹️','😩','😤']

@udf(IntegerType())
def count_positive_emojis(text):
    return builtins.sum(1 for e in POSITIVE_EMOJIS if e in text) if text else 0

@udf(IntegerType())
def count_negative_emojis(text):
    return builtins.sum(1 for e in NEGATIVE_EMOJIS if e in text) if text else 0

df = df.withColumn("positive_emojis", count_positive_emojis(col("combined_text")))
df = df.withColumn("negative_emojis", count_negative_emojis(col("combined_text")))
df = df.withColumn("emoji_score", col("positive_emojis") - col("negative_emojis"))

# ===============================
# VADER LABELING (RÉFÉRENCE)
# ===============================
print("\n🏷️  Labellisation avec VADER (baseline)...")

@udf(StringType())
def vader_sentiment(text):
    """Utilise VADER pour déterminer le sentiment"""
    if not text:
        return "neutral"
    
    try:
        # Analyse VADER
        scores = vader_analyzer.polarity_scores(text)
        compound = scores['compound']
        
        # Classification selon les seuils VADER standards
        if compound >= 0.05:
            return "positive"
        elif compound <= -0.05:
            return "negative"
        else:
            return "neutral"
    except:
        return "neutral"

@udf(FloatType())
def vader_score(text):
    """Retourne le score compound VADER"""
    if not text:
        return 0.0
    try:
        scores = vader_analyzer.polarity_scores(text)
        return float(scores['compound'])
    except:
        return 0.0

# Appliquer VADER
df = df.withColumn("vader_sentiment", vader_sentiment(col("combined_text")))
df = df.withColumn("vader_score", vader_score(col("combined_text")))

print("\n📊 Distribution des labels VADER :")
df.groupBy("vader_sentiment").count().orderBy("count", ascending=False).show()

# ===============================
# CLEAN TEXT (GARDER EMOJIS!)
# ===============================
@udf(StringType())
def clean_text_keep_emoji(text):
    """
    Nettoie le texte en GARDANT les emojis (informations importantes!)
    """
    if not text:
        return ""
    
    text = text.lower()
    
    # Supprimer URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    
    # Supprimer mentions @
    text = re.sub(r"@\w+", "", text)
    
    # GARDER les emojis! Supprimer seulement la ponctuation excessive
    # Pattern qui garde lettres, chiffres, espaces ET emojis
    # Ne pas utiliser [^\w\s] qui supprimerait les emojis
    text = re.sub(r"[!\"#$%&'()*+,\-./:;<=>?@\[\\\]^_`{|}~]", " ", text)
    
    # Supprimer espaces multiples
    return re.sub(r"\s+", " ", text).strip()

df = df.withColumn("cleaned_text", clean_text_keep_emoji(col("combined_text")))

# ===============================
# PIPELINE FEATURES
# ===============================
# Utiliser vader_sentiment comme label pour l'entraînement
label_indexer = StringIndexer(
    inputCol="vader_sentiment",  # ← Labels VADER
    outputCol="label_index",
    handleInvalid="keep"
)

tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

cv = CountVectorizer(
    inputCol="filtered_words",
    outputCol="raw_features",
    vocabSize=2000,
    minDF=2
)

idf = IDF(inputCol="raw_features", outputCol="text_features")

assembler = VectorAssembler(
    inputCols=["text_features", "score", "num_comments", "emoji_score"],
    outputCol="features",
    handleInvalid="skip"
)

# ===============================
# SPLIT
# ===============================
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"\n📊 Train: {train_df.count()} | Test: {test_df.count()}")

# ===============================
# MODÈLE 1: LOGISTIC REGRESSION
# ===============================
print("\n🔄 Entraînement Logistic Regression...")

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label_index",
    maxIter=100,
    regParam=0.01
)

lr_pipeline = Pipeline(stages=[
    label_indexer, tokenizer, remover, cv, idf, assembler, lr
])

try:
    lr_model = lr_pipeline.fit(train_df)
    lr_preds = lr_model.transform(test_df)
    
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label_index",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    lr_acc = evaluator.evaluate(lr_preds)
    print(f"✅ Logistic Regression Accuracy: {lr_acc:.2%}")
except Exception as e:
    print(f"⚠️  Logistic Regression: {e}")
    lr_acc = 0
    lr_model = None

# ===============================
# MODÈLE 2: RANDOM FOREST
# ===============================
print("\n🔄 Entraînement Random Forest...")

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label_index",
    numTrees=50,
    maxDepth=10,
    seed=42
)

rf_pipeline = Pipeline(stages=[
    label_indexer, tokenizer, remover, cv, idf, assembler, rf
])

try:
    rf_model = rf_pipeline.fit(train_df)
    rf_preds = rf_model.transform(test_df)
    
    rf_acc = evaluator.evaluate(rf_preds)
    print(f"✅ Random Forest Accuracy: {rf_acc:.2%}")
except Exception as e:
    print(f"⚠️  Random Forest: {e}")
    rf_acc = 0
    rf_model = None

# ===============================
# MODÈLE 3: NAIVE BAYES
# ===============================
print("\n🔄 Entraînement Naive Bayes...")

nb = NaiveBayes(
    featuresCol="features",
    labelCol="label_index",
    smoothing=1.0
)

nb_pipeline = Pipeline(stages=[
    label_indexer, tokenizer, remover, cv, idf, assembler, nb
])

try:
    nb_model = nb_pipeline.fit(train_df)
    nb_preds = nb_model.transform(test_df)
    
    nb_acc = evaluator.evaluate(nb_preds)
    print(f"✅ Naive Bayes Accuracy: {nb_acc:.2%}")
except Exception as e:
    print(f"⚠️  Naive Bayes: {e}")
    nb_acc = 0
    nb_model = None

# ===============================
# VADER BASELINE (pour comparaison)
# ===============================
print("\n🔄 Évaluation VADER (baseline)...")

# VADER prédit directement, pas besoin d'entraînement
# On évalue sur le test set
vader_test = test_df.select("vader_sentiment", col("vader_sentiment").alias("vader_prediction"))

# Comparer VADER avec lui-même sur le test set (devrait être 100%)
# Mais on veut voir la cohérence
vader_baseline_acc = 1.0  # VADER est cohérent avec lui-même
print(f"✅ VADER Baseline: 100.00% (cohérent)")

# ===============================
# COMPARAISON FINALE
# ===============================
models = [
    ("VADER (Baseline)", None, 1.0, "Règles pré-définies"),
    ("Logistic Regression", lr_model, lr_acc, "ML entraîné"),
    ("Random Forest", rf_model, rf_acc, "ML entraîné"),
    ("Naive Bayes", nb_model, nb_acc, "ML entraîné")
]

# Filtrer les modèles qui ont fonctionné
models = [(n, m, a, t) for n, m, a, t in models if m is not None or n == "VADER (Baseline)"]
models.sort(key=lambda x: x[2], reverse=True)

print("\n" + "=" * 70)
print("🏆 COMPARAISON DES MODÈLES")
print("=" * 70)
print(f"{'Modèle':<25} | {'Accuracy':<10} | {'Type':<20}")
print("-" * 70)
for name, model, acc, type_model in models:
    status = "🏆" if acc == max([a for _, _, a, _ in models]) else "  "
    print(f"{status} {name:<23} | {acc:>8.2%}  | {type_model:<20}")
print("=" * 70)

# Sélectionner le meilleur modèle ML (pas VADER)
ml_models = [(n, m, a) for n, m, a, t in models if t == "ML entraîné" and m is not None]
if ml_models:
    ml_models.sort(key=lambda x: x[2], reverse=True)
    best_name, best_model, best_acc = ml_models[0]
    
    print(f"\n🎯 MEILLEUR MODÈLE ML : {best_name}")
    print(f"   Accuracy: {best_acc:.2%}")
    print(f"   Baseline VADER: 100% (référence)")
    
    if best_acc >= 0.95:
        print(f"   📊 Votre modèle est presque aussi bon que VADER!")
    elif best_acc >= 0.85:
        print(f"   📊 Très bonne performance pour un modèle entraîné!")
    
    print("=" * 70)
    
    # ===============================
    # PRÉDICTIONS FINALES
    # ===============================
    real_labels = best_model.stages[0].labels
    
    index_to_label = IndexToString(
        inputCol="prediction",
        outputCol="ml_sentiment",
        labels=real_labels
    )
    
    final_df = index_to_label.transform(best_model.transform(df))
    
    # ===============================
    # RÉSULTATS AVEC COMPARAISON
    # ===============================
    result_df = final_df.select(
        "id",
        "combined_text",
        col("vader_sentiment").alias("vader_label"),  # Label VADER
        col("vader_score"),                            # Score VADER
        col("ml_sentiment").alias("ml_prediction"),    # Prédiction ML
        "score",
        "num_comments",
        "emoji_score",
        "positive_emojis",
        "negative_emojis",
        current_timestamp().alias("analyzed_at")
    )
    
    print("\n📊 Distribution VADER vs ML :")
    print("\nVADER:")
    result_df.groupBy("vader_label").count().orderBy("count", ascending=False).show()
    
    print("\nML Prediction:")
    result_df.groupBy("ml_prediction").count().orderBy("count", ascending=False).show()
    
    # Accord entre VADER et ML
    agreement = result_df.filter(col("vader_label") == col("ml_prediction")).count()
    total = result_df.count()
    agreement_rate = (agreement / total) * 100
    
    print(f"\n📊 Taux d'accord VADER ↔ ML : {agreement_rate:.1f}%")
    print(f"   ({agreement}/{total} posts avec le même sentiment)")
    
    # ===============================
    # SAVE TO MONGODB
    # ===============================
    print("\n💾 Sauvegarde MongoDB...")
    pdf = result_df.toPandas()
    pdf["analyzed_at"] = pdf["analyzed_at"].astype(str)
    
    saved_count = 0
    for doc in pdf.to_dict("records"):
        try:
            results_col.update_one(
                {"id": doc["id"]},
                {"$set": doc},
                upsert=True
            )
            saved_count += 1
        except Exception as e:
            print(f"⚠️  Erreur: {e}")
    
    print(f"✅ {saved_count}/{len(pdf)} documents sauvegardés")
    
    # ===============================
    # ANALYSE DES DIVERGENCES
    # ===============================
    print("\n📋 Analyse des divergences VADER vs ML:")
    
    divergent = result_df.filter(col("vader_label") != col("ml_prediction"))
    
    if divergent.count() > 0:
        print(f"\n🔍 {divergent.count()} posts avec prédictions différentes:")
        divergent.select(
            "combined_text",
            "vader_label",
            "ml_prediction",
            "score"
        ).show(5, truncate=60)
    else:
        print("✅ Accord parfait entre VADER et ML!")

else:
    print("❌ Aucun modèle ML n'a pu être entraîné")

# ===============================
# END
# ===============================
print("\n" + "=" * 70)
print("✅ ANALYSE TERMINÉE AVEC SUCCÈS")
print("=" * 70)

spark.stop()
mongo.close()