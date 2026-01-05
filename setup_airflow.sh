#!/bin/bash

echo "=========================================="
echo "🚀 SETUP AIRFLOW - Reddit CAN 2025"
echo "=========================================="

# Créer la structure des dossiers Airflow
echo ""
echo "📁 Création de la structure Airflow..."
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p airflow/config

# Définir l'UID Airflow (important pour les permissions)
echo ""
echo "🔑 Configuration des permissions..."
export AIRFLOW_UID=$(id -u)
echo "AIRFLOW_UID=$AIRFLOW_UID" > .env

# Copier le DAG dans le dossier dags
echo ""
echo "📋 Copie du DAG principal..."
if [ -f "reddit_can_pipeline_dag.py" ]; then
    cp reddit_can_pipeline_dag.py airflow/dags/
    echo "✅ DAG copié vers airflow/dags/"
else
    echo "⚠️  Fichier reddit_can_pipeline_dag.py non trouvé"
fi

# Build des images Docker nécessaires
echo ""
echo "🐳 Build des images Docker..."
echo "   → Scraper..."
docker-compose build scraper-producer

echo "   → Spark ML..."
docker-compose build spark-ml-sentiment

echo "   → Dashboard..."
docker-compose build dashboard

echo ""
echo "✅ Images buildées!"

# Initialiser Airflow
echo ""
echo "🔧 Initialisation d'Airflow..."
docker-compose up airflow-init

# Démarrer tous les services
echo ""
echo "🚀 Démarrage de tous les services..."
docker-compose up -d

# Attendre que tout soit prêt
echo ""
echo "⏳ Attente du démarrage complet (30s)..."
sleep 30

# Afficher l'état
echo ""
echo "📊 État des services:"
docker-compose ps

echo ""
echo "=========================================="
echo "✅ SETUP TERMINÉ!"
echo "=========================================="
echo ""
echo "🌐 ACCÈS AUX INTERFACES:"
echo "   → Airflow:        http://localhost:8080"
echo "   → Dashboard:      http://localhost:8501"
echo "   → Mongo Express:  http://localhost:8081"
echo ""
echo "🔐 Credentials Airflow:"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "📋 Commandes utiles:"
echo "   docker-compose logs -f airflow-scheduler"
echo "   docker-compose logs -f scraper-producer"
echo "   docker-compose logs -f spark-streaming"
echo ""
echo "🎯 Pour déclencher manuellement le DAG:"
echo "   docker exec -it airflow-scheduler airflow dags trigger reddit_can_pipeline"
echo "=========================================="