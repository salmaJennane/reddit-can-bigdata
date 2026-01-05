"""
========================================================
🤖 SPARK ML FINAL - SENTIMENT ANALYSIS CAN 2025
========================================================
✔ Auto-labeling CORRIGÉ (mots-clés tragiques ajoutés)
✔ Pondération score Reddit améliorée
✔ Emojis + engagement
✔ Multi-modèles avec comparaison
✔ Mapping CORRECT des labels
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
from pyspark.sql.types import StringType, IntegerType
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
import builtins
import re

# ===============================
# SPARK SESSION
# ===============================
print("=" * 70)
print("🤖 SPARK ML FINAL - SENTIMENT ANALYSIS CAN 2025")
print("=" * 70)

spark = SparkSession.builder \
    .appName("RedditSentimentMLFinal") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session créée!")

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
# EMOJIS
# ===============================
POSITIVE_EMOJIS = ['😊','😃','😁','😍','🥰','👏','👍','🔥','❤️','🏆','🥇','🎯','✨','💪']
NEGATIVE_EMOJIS = ['😢','😭','😞','😡','🤬','💔','👎','❌','😰','☹️','😩']

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
# AUTO-LABELING AMÉLIORÉ
# ===============================
@udf(StringType())
def auto_label_improved(text, score, comments, emoji_score):
    """
    Auto-labeling amélioré avec détection des tragédies
    """
    if not text:
        return "neutral"

    text_lower = text.lower()
    
    # ========================================
    # MOTS-CLÉS POSITIFS (Football/CAN)
    # ========================================
    positive_words = [
        'win', 'won', 'victory', 'champion', 'champions', 'qualify', 'qualified',
        'great', 'amazing', 'excellent', 'brilliant', 'fantastic',
        'love', 'best', 'perfect', 'hero', 'legend', 'historic',
        'celebrate', 'celebration', 'goal', 'goals', 'dominant',
        'unstoppable', 'impressive', 'proud', 'congrat'
    ]
    
    # ========================================
    # MOTS-CLÉS NÉGATIFS (CORRIGÉ !)
    # ========================================
    negative_words = [
        # Défaites sportives
        'lose', 'lost', 'defeat', 'defeated', 'fail', 'failed',
        'exit', 'eliminated', 'elimination', 'disappoint',
        'terrible', 'worst', 'poor', 'awful', 'weak',
        'shame', 'embarrass', 'pathetic', 'useless',
        
        # AJOUT CRUCIAL : Tragédies et Violence
        'kill', 'killed', 'killing', 'death', 'dead', 'die', 'died',
        'war', 'civil war', 'conflict', 'violence', 'violent',
        'shot', 'shoot', 'shooting', 'murdered', 'murder',
        'attack', 'attacked', 'victim', 'victims', 'tragedy', 'tragic',
        'crisis', 'disaster', 'horror', 'horrific', 'terror',
        'threat', 'threaten', 'danger', 'dangerous'
    ]
    
    # Compter les occurrences
    pos_count = builtins.sum(1 for w in positive_words if w in text_lower)
    neg_count = builtins.sum(1 for w in negative_words if w in text_lower)
    
    # Score initial basé sur les mots
    total_score = (pos_count - neg_count) * 3
    
    # Ajouter le score des emojis
    if emoji_score:
        total_score += emoji_score * 2
    
    # ========================================
    # PONDÉRATION SCORE REDDIT (AMÉLIORÉE)
    # ========================================
    # Score élevé ne signifie pas forcément positif !
    # Un post tragique peut être très upvoté car important
    if score:
        # Seulement si pas de mots négatifs forts
        if neg_count == 0:
            if score > 100:
                total_score += 1  # Réduit de 2 → 1
            elif score < -5:
                total_score -= 2
        else:
            # Si mots négatifs présents, le score ne compte pas
            if score < -5:
                total_score -= 2
    
    # Bonus engagement (si beaucoup de commentaires)
    if comments and comments > 50:
        total_score += 1
    
    # ========================================
    # DÉCISION FINALE
    # ========================================
    if total_score > 2:
        return "positive"
    elif total_score < -2:
        return "negative"
    else:
        return "neutral"

df = df.withColumn(
    "label",
    auto_label_improved(
        col("combined_text"),
        col("score"),
        col("num_comments"),
        col("emoji_score")
    )
)

print("\n📊 Distribution des labels :")
df.groupBy("label").count().orderBy("count", ascending=False).show()

# ===============================
# CLEAN TEXT
# ===============================
@udf(StringType())
def clean_text(text):
    if not text:
        return ""
    text = text.lower()
    # Supprimer URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    # Garder lettres, chiffres, espaces et emojis
    text = re.sub(r"[^\w\s😀-🙏]", " ", text)
    # Supprimer espaces multiples
    return re.sub(r"\s+", " ", text).strip()

df = df.withColumn("cleaned_text", clean_text(col("combined_text")))

# ===============================
# PIPELINE FEATURES
# ===============================
label_indexer = StringIndexer(
    inputCol="label",
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
# MODÈLE 2: RANDOM FOREST (BEST)
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
# COMPARAISON & SÉLECTION
# ===============================
models = [
    ("Logistic Regression", lr_model, lr_acc),
    ("Random Forest", rf_model, rf_acc),
    ("Naive Bayes", nb_model, nb_acc)
]

# Trier par accuracy
models = [(n, m, a) for n, m, a in models if m is not None]
models.sort(key=lambda x: x[2], reverse=True)

if not models:
    print("❌ Aucun modèle n'a pu être entraîné")
    exit(1)

best_name, best_model, best_acc = models[0]

print("\n" + "=" * 70)
print("🏆 COMPARAISON DES MODÈLES")
print("=" * 70)
for name, model, acc in models:
    status = "🏆" if name == best_name else "  "
    print(f"{status} {name:25s} | Accuracy: {acc:6.2%}")
print("=" * 70)
print(f"\n🎯 MEILLEUR MODÈLE : {best_name}")
print(f"   Accuracy: {best_acc:.2%}")
print("=" * 70)

# ===============================
# MAPPING CORRECT DES LABELS
# ===============================
real_labels = best_model.stages[0].labels
print(f"\n🔎 Ordre réel des labels Spark : {real_labels}")

index_to_label = IndexToString(
    inputCol="prediction",
    outputCol="predicted_sentiment",
    labels=real_labels
)

final_df = index_to_label.transform(best_model.transform(df))

# ===============================
# RÉSULTATS FINAUX
# ===============================
result_df = final_df.select(
    "id",
    "combined_text",
    col("label").alias("true_sentiment"),
    "predicted_sentiment",
    "score",
    "num_comments",
    "emoji_score",
    "positive_emojis",
    "negative_emojis",
    current_timestamp().alias("analyzed_at")
)

print("\n📊 Distribution finale des prédictions :")
result_df.groupBy("predicted_sentiment").count().orderBy("count", ascending=False).show()

print("\n📋 Exemples de prédictions (posts avec tragédies) :")
tragedy_posts = result_df.filter(
    col("combined_text").contains("kill") | 
    col("combined_text").contains("war") |
    col("combined_text").contains("death")
)

if tragedy_posts.count() > 0:
    print("🔍 Posts tragiques détectés :")
    tragedy_posts.select(
        "combined_text",
        "predicted_sentiment",
        "score"
    ).show(5, truncate=60)

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
# STATISTIQUES FINALES
# ===============================
print("\n" + "=" * 70)
print("📊 STATISTIQUES FINALES")
print("=" * 70)
print(f"Total posts analysés:     {len(pdf)}")
print(f"Accuracy finale:          {best_acc:.2%}")
print(f"Modèle utilisé:           {best_name}")
print("=" * 70)

# Vérifier la correction
if tragedy_posts.count() > 0:
    neg_count = tragedy_posts.filter(col("predicted_sentiment") == "negative").count()
    total = tragedy_posts.count()
    print(f"\n🔍 Vérification tragédies:")
    print(f"   Posts tragiques détectés: {total}")
    print(f"   Classés négatifs:         {neg_count} ({neg_count/total*100:.1f}%)")
    print("=" * 70)

print("\n✅ ANALYSE TERMINÉE AVEC SUCCÈS")
print("=" * 70)

spark.stop()
mongo.close()