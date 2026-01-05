# Utiliser l'image Spark existante
FROM projet-bigdata-can-spark-streaming:latest

WORKDIR /app

# Installer packages Python supplémentaires
RUN pip install --no-cache-dir \
    pandas>=1.0.5 \
    nltk==3.8.1 \
    scikit-learn==1.3.2 \
    pymongo==4.5.0

# Télécharger les données NLTK nécessaires
RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('punkt'); nltk.download('vader_lexicon')"

# Copier le script dans le container
COPY spark_ml_sentiment.py .

# Commande par défaut
CMD ["python", "-u", "spark_ml_sentiment.py"]
