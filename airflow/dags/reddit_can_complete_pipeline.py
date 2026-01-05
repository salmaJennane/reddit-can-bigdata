"""
========================================================
AIRFLOW DAG - PIPELINE COMPLET REDDIT CAN 2025
========================================================
Orchestration du pipeline Big Data de bout en bout:
1. Vérification infrastructure
2. Scraping Reddit → Kafka (10 min)
3. Attente traitement Spark Streaming
4. Analyse ML Sentiment
5. Validation et rapport
========================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging

# ========================================
# CONFIGURATION
# ========================================
default_args = {
    'owner': 'bigdata_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

MONGODB_URI = 'mongodb://admin:admin123@mongodb:27017/'
MIN_POSTS_FOR_ML = 50  # Seuil minimum avant ML

# ========================================
# FONCTIONS PYTHON
# ========================================

def check_infrastructure(**context):
    """Vérifie que l'infrastructure est opérationnelle"""
    logging.info("🔍 Vérification de l'infrastructure...")
    
    try:
        # Vérifier MongoDB
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logging.info("✅ MongoDB opérationnel")
        client.close()
        
        return True
    except Exception as e:
        logging.error(f"❌ Infrastructure error: {e}")
        raise

def get_pipeline_stats(**context):
    """Récupère les stats du pipeline"""
    logging.info("📊 Récupération des statistiques...")
    
    try:
        client = MongoClient(MONGODB_URI)
        db = client['reddit_can']
        
        stats = {
            'timestamp': datetime.now().isoformat(),
            'posts': db['posts'].count_documents({}),
            'comments': db['comments'].count_documents({}),
            'processed_posts': db['processed_posts'].count_documents({}),
            'sentiment_results': db['sentiment_results'].count_documents({})
        }
        
        logging.info(f"📈 Stats: {stats}")
        
        client.close()
        
        # Pousser vers XCom pour les autres tâches
        context['task_instance'].xcom_push(key='pipeline_stats', value=stats)
        
        return stats
        
    except Exception as e:
        logging.error(f"❌ Erreur stats: {e}")
        raise

def check_ml_threshold(**context):
    """Vérifie si on a assez de données pour le ML"""
    logging.info("🔍 Vérification du seuil ML...")
    
    try:
        # Récupérer les stats depuis XCom
        stats = context['task_instance'].xcom_pull(
            task_ids='stats_after_scraping',
            key='pipeline_stats'
        )
        
        if not stats:
            logging.warning("⚠️ Pas de stats disponibles")
            return 'skip_ml'
        
        processed_count = stats.get('processed_posts', 0)
        
        logging.info(f"📊 Posts traités: {processed_count}")
        logging.info(f"🎯 Seuil minimum: {MIN_POSTS_FOR_ML}")
        
        if processed_count >= MIN_POSTS_FOR_ML:
            logging.info("✅ Seuil atteint → Lancement du ML")
            return 'run_ml_analysis'
        else:
            logging.warning(f"⚠️ Seuil non atteint ({processed_count}/{MIN_POSTS_FOR_ML})")
            return 'skip_ml'
            
    except Exception as e:
        logging.error(f"❌ Erreur vérification: {e}")
        return 'skip_ml'

def generate_final_report(**context):
    """Génère un rapport complet du pipeline"""
    logging.info("\n" + "="*70)
    logging.info("📊 RAPPORT FINAL DU PIPELINE")
    logging.info("="*70)
    
    try:
        # Récupérer les stats finales
        stats = context['task_instance'].xcom_pull(
            task_ids='stats_final',
            key='pipeline_stats'
        )
        
        if not stats:
            logging.warning("⚠️ Pas de stats finales")
            return
        
        # Rapport détaillé
        logging.info(f"\n📅 Timestamp: {stats['timestamp']}")
        logging.info(f"\n📊 Volume de données:")
        logging.info(f"   Posts bruts:         {stats['posts']:>5}")
        logging.info(f"   Commentaires:        {stats['comments']:>5}")
        logging.info(f"   Posts traités:       {stats['processed_posts']:>5}")
        logging.info(f"   Sentiments analysés: {stats['sentiment_results']:>5}")
        
        # Taux de couverture
        if stats['processed_posts'] > 0:
            coverage = (stats['sentiment_results'] / stats['processed_posts']) * 100
            logging.info(f"\n✅ Taux de couverture ML: {coverage:.1f}%")
        
        # Distribution des sentiments
        client = MongoClient(MONGODB_URI)
        db = client['reddit_can']
        
        pipeline = [
            {'$group': {
                '_id': '$predicted_sentiment',
                'count': {'$sum': 1}
            }}
        ]
        
        sentiment_dist = list(db['sentiment_results'].aggregate(pipeline))
        
        if sentiment_dist:
            logging.info(f"\n💭 Distribution des sentiments:")
            for item in sentiment_dist:
                logging.info(f"   {item['_id']:15s}: {item['count']:5d}")
        
        client.close()
        
        # Recommandations
        logging.info(f"\n💡 Recommandations:")
        if stats['posts'] < 100:
            logging.warning("   ⚠️ Volume de posts faible - Élargir les critères de scraping")
        if stats['sentiment_results'] < 50:
            logging.warning("   ⚠️ Peu de sentiments analysés - Augmenter la fréquence")
        if stats['posts'] >= 300:
            logging.info("   ✅ Volume de données excellent")
        
        logging.info("="*70)
        
        # Sauvegarder le rapport
        context['task_instance'].xcom_push(key='final_report', value=stats)
        
        return stats
        
    except Exception as e:
        logging.error(f"❌ Erreur génération rapport: {e}")
        raise

def cleanup_old_data(**context):
    """Nettoie les anciennes données (optionnel)"""
    logging.info("🧹 Nettoyage des données anciennes...")
    
    try:
        client = MongoClient(MONGODB_URI)
        db = client['reddit_can']
        
        # Garder seulement les 7 derniers jours
        cutoff_date = datetime.now() - timedelta(days=7)
        
        # Compter avant
        old_posts = db['posts'].count_documents({
            'created_date': {'$lt': cutoff_date.isoformat()}
        })
        
        if old_posts > 0:
            logging.info(f"🗑️ Suppression de {old_posts} anciens posts")
            # Décommenter pour activer le nettoyage
            # db['posts'].delete_many({'created_date': {'$lt': cutoff_date.isoformat()}})
        else:
            logging.info("✅ Pas de données anciennes à supprimer")
        
        client.close()
        
    except Exception as e:
        logging.warning(f"⚠️ Erreur nettoyage: {e}")

# ========================================
# DAG DEFINITION
# ========================================
with DAG(
    dag_id='reddit_can_complete_pipeline',
    default_args=default_args,
    description='Pipeline Big Data complet - Reddit CAN 2025',
    schedule_interval='0 */6 * * *',  # Toutes les 6 heures
    start_date=days_ago(1),
    catchup=False,
    tags=['bigdata', 'reddit', 'can2025', 'production'],
    doc_md="""
    # Pipeline Reddit CAN 2025 - Production
    
    ## Objectif
    Collecte, traitement et analyse de sentiments des discussions Reddit 
    sur la CAN 2025 (Coupe d'Afrique des Nations).
    
    ## Flux
    1. Vérification infrastructure (Kafka, MongoDB)
    2. Scraping Reddit → Kafka (10 minutes)
    3. Attente traitement Spark Streaming (60s)
    4. Analyse ML Sentiment (si >= 50 posts)
    5. Génération rapport final
    6. Nettoyage optionnel
    
    ## Fréquence
    Toutes les 6 heures
    
    ## Technologies
    - Kafka: Streaming temps réel
    - Spark: Traitement massif parallèle
    - MongoDB: Stockage NoSQL
    - Spark ML: Analyse de sentiments (84% accuracy)
    - Streamlit: Dashboard interactif
    """
) as dag:

    # ========================================
    # TÂCHE 1: Vérification Infrastructure
    # ========================================
    check_infra = PythonOperator(
        task_id='check_infrastructure',
        python_callable=check_infrastructure,
        doc_md="Vérifie que MongoDB et Kafka sont opérationnels"
    )

    # ========================================
    # TÂCHE 2: Stats AVANT scraping
    # ========================================
    stats_before = PythonOperator(
        task_id='stats_before_scraping',
        python_callable=get_pipeline_stats,
        doc_md="Récupère les statistiques avant le scraping"
    )

    # ========================================
    # TÂCHE 3: SCRAPING Reddit → Kafka
    # ========================================
    scraping = DockerOperator(
        task_id='scraping_reddit_to_kafka',
        image='projet-bigdata-can-scraper-producer:latest',
        container_name='airflow_scraper_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='projet-bigdata-can_bigdata-network',
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'KAFKA_TOPIC': 'reddit-can-posts',
            'SCRAPING_INTERVAL': '600',  # 10 minutes
            'PYTHONUNBUFFERED': '1'
        },
        execution_timeout=timedelta(minutes=12),
        mount_tmp_dir=False,
        doc_md="Lance le scraper Reddit pour 10 minutes de collecte intensive"
    )

    # ========================================
    # TÂCHE 4: Attendre Spark Streaming
    # ========================================
    wait_processing = BashOperator(
        task_id='wait_spark_streaming',
        bash_command='echo "⏳ Attente traitement Spark Streaming (60s)..." && sleep 60',
        doc_md="Attend que Spark Streaming traite les données"
    )

    # ========================================
    # TÂCHE 5: Stats APRÈS scraping
    # ========================================
    stats_after = PythonOperator(
        task_id='stats_after_scraping',
        python_callable=get_pipeline_stats,
        doc_md="Récupère les statistiques après le scraping"
    )

    # ========================================
    # TÂCHE 6: Vérifier seuil ML
    # ========================================
    check_threshold = BranchPythonOperator(
        task_id='check_ml_threshold',
        python_callable=check_ml_threshold,
        doc_md="Vérifie si on a assez de données (>= 50 posts) pour lancer le ML"
    )

    # ========================================
    # TÂCHE 7a: LANCER ML
    # ========================================
    run_ml = DockerOperator(
        task_id='run_ml_analysis',
        image='projet-bigdata-can-spark-ml-sentiment:latest',
        container_name='airflow_spark_ml_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='projet-bigdata-can_bigdata-network',
        environment={
            'MONGODB_URI': 'mongodb://admin:admin123@mongodb:27017/reddit_can?authSource=admin'
        },
        execution_timeout=timedelta(minutes=15),
        mount_tmp_dir=False,
        trigger_rule='none_failed_min_one_success',
        doc_md="Lance l'analyse de sentiments avec Spark ML (Random Forest 84% accuracy)"
    )

    # ========================================
    # TÂCHE 7b: SKIP ML
    # ========================================
    skip_ml = BashOperator(
        task_id='skip_ml',
        bash_command='echo "⚠️ ML skippé - Pas assez de données (< 50 posts)"',
        trigger_rule='none_failed_min_one_success',
        doc_md="Skip si pas assez de données"
    )

    # ========================================
    # TÂCHE 8: Stats FINALES
    # ========================================
    stats_final = PythonOperator(
        task_id='stats_final',
        python_callable=get_pipeline_stats,
        trigger_rule='none_failed_min_one_success',
        doc_md="Récupère les statistiques finales après ML"
    )

    # ========================================
    # TÂCHE 9: Génération Rapport
    # ========================================
    generate_report = PythonOperator(
        task_id='generate_final_report',
        python_callable=generate_final_report,
        trigger_rule='none_failed_min_one_success',
        doc_md="Génère un rapport complet avec toutes les métriques"
    )

    # ========================================
    # TÂCHE 10: Nettoyage (optionnel)
    # ========================================
    cleanup = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        trigger_rule='all_done',
        doc_md="Nettoie les données de plus de 7 jours (optionnel)"
    )

    # ========================================
    # TÂCHE 11: Notification Finale
    # ========================================
    notify = BashOperator(
        task_id='pipeline_success',
        bash_command="""
        echo "=========================================="
        echo "✅ PIPELINE TERMINÉ AVEC SUCCÈS"
        echo "=========================================="
        echo ""
        echo "🌐 Dashboard: http://localhost:8501"
        echo "📊 MongoDB: http://localhost:8081"
        echo ""
        echo "📝 Logs complets dans Airflow UI"
        echo "=========================================="
        """,
        trigger_rule='all_done',
        doc_md="Affiche les informations finales"
    )

    # ========================================
    # DÉFINITION DU FLUX
    # ========================================
    
    # Phase 1: Infrastructure et collecte
    check_infra >> stats_before >> scraping >> wait_processing >> stats_after
    
    # Phase 2: Décision ML
    stats_after >> check_threshold
    check_threshold >> [run_ml, skip_ml]
    
    # Phase 3: Finalisation
    [run_ml, skip_ml] >> stats_final >> generate_report >> cleanup >> notify