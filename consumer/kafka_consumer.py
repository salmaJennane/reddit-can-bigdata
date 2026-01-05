"""
Kafka Consumer - Lit depuis Kafka et sauvegarde dans MongoDB
"""

import json
import os
import sys
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# Force flush des prints immédiatement
sys.stdout.flush()

class RedditKafkaConsumer:
    def __init__(self):
        # Configuration Kafka
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'reddit-can-posts')
        
        # Configuration MongoDB
        mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@localhost:27017/reddit_can?authSource=admin')
        
        print("="*60, flush=True)
        print("🎯 REDDIT KAFKA CONSUMER - INITIALISATION", flush=True)
        print("="*60, flush=True)
        print(f"📍 Kafka: {kafka_servers}", flush=True)
        print(f"📍 Topic: {self.topic}", flush=True)
        print(f"📍 MongoDB: {mongodb_uri.split('@')[1] if '@' in mongodb_uri else mongodb_uri}", flush=True)
        
        # Attendre Kafka
        self._wait_for_kafka(kafka_servers)
        
        # Initialiser le consumer Kafka
        print(f"🔌 Création du consumer Kafka...", flush=True)
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=kafka_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='reddit-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print("✅ Connecté à Kafka!", flush=True)
        
        # Initialiser MongoDB
        print(f"🔌 Connexion à MongoDB...", flush=True)
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client['reddit_can']
        self.posts_collection = self.db['posts']
        self.comments_collection = self.db['comments']
        
        # Créer des index pour éviter les doublons
        self.posts_collection.create_index('id', unique=True)
        self.comments_collection.create_index('id', unique=True)
        
        print("✅ Connecté à MongoDB!", flush=True)
        print("="*60, flush=True)
        
        # Statistiques
        self.stats = {
            'posts_saved': 0,
            'comments_saved': 0,
            'duplicates': 0,
            'errors': 0
        }
    
    def _wait_for_kafka(self, kafka_servers, timeout=60):
        """Attendre que Kafka soit prêt"""
        print("⏳ Attente de Kafka...", flush=True)
        start = time.time()
        
        while time.time() - start < timeout:
            try:
                from kafka import KafkaAdminClient
                admin = KafkaAdminClient(
                    bootstrap_servers=kafka_servers,
                    request_timeout_ms=5000
                )
                admin.close()
                print("✅ Kafka est prêt!", flush=True)
                return True
            except Exception as e:
                elapsed = int(time.time() - start)
                print(f"⏳ Kafka pas encore prêt... ({elapsed}s)", flush=True)
                time.sleep(5)
        
        raise Exception("❌ Timeout: Kafka n'est pas accessible")
    
    def save_to_mongodb(self, data):
        """Sauvegarde les données dans MongoDB"""
        try:
            # Ajouter timestamp de sauvegarde
            data['saved_at'] = datetime.now().isoformat()
            
            if data['type'] == 'post':
                # Sauvegarder dans la collection posts
                self.posts_collection.insert_one(data)
                self.stats['posts_saved'] += 1
                print(f"✅ Post sauvegardé: {data['title'][:50]}...", flush=True)
                
            elif data['type'] == 'comment':
                # Sauvegarder dans la collection comments
                self.comments_collection.insert_one(data)
                self.stats['comments_saved'] += 1
                print(f"✅ Comment sauvegardé (post_id: {data['post_id']})", flush=True)
            
            return True
            
        except DuplicateKeyError:
            # Document déjà existant (doublon)
            self.stats['duplicates'] += 1
            print(f"⚠️  Doublon ignoré: {data.get('id', 'unknown')}", flush=True)
            return False
            
        except Exception as e:
            print(f"❌ Erreur MongoDB: {e}", flush=True)
            self.stats['errors'] += 1
            return False
    
    def consume_messages(self):
        """Consomme les messages depuis Kafka"""
        print("\n🚀 DÉMARRAGE DE LA CONSOMMATION", flush=True)
        print("="*60, flush=True)
        print("⏳ En attente de messages...\n", flush=True)
        
        message_count = 0
        
        try:
            for message in self.consumer:
                message_count += 1
                print(f"\n📨 Message #{message_count} reçu", flush=True)
                
                data = message.value
                
                # Sauvegarder dans MongoDB
                self.save_to_mongodb(data)
                
                # Afficher les stats toutes les 10 insertions
                total = self.stats['posts_saved'] + self.stats['comments_saved']
                if total % 10 == 0 and total > 0:
                    self.print_stats()
                    
        except KeyboardInterrupt:
            print("\n\n🛑 Arrêt demandé par l'utilisateur", flush=True)
            self.close()
        except Exception as e:
            print(f"\n❌ Erreur: {e}", flush=True)
            self.stats['errors'] += 1
            time.sleep(5)
    
    def print_stats(self):
        """Affiche les statistiques"""
        print("\n" + "="*60, flush=True)
        print("📊 STATISTIQUES", flush=True)
        print("="*60, flush=True)
        print(f"Posts sauvegardés:     {self.stats['posts_saved']}", flush=True)
        print(f"Comments sauvegardés:  {self.stats['comments_saved']}", flush=True)
        print(f"Doublons ignorés:      {self.stats['duplicates']}", flush=True)
        print(f"Erreurs:               {self.stats['errors']}", flush=True)
        print("="*60 + "\n", flush=True)
    
    def close(self):
        """Ferme proprement les connexions"""
        print("\n🔌 Fermeture des connexions...", flush=True)
        self.print_stats()
        self.consumer.close()
        self.mongo_client.close()
        print("✅ Connexions fermées", flush=True)

if __name__ == "__main__":
    print("🚀 Démarrage du consumer...", flush=True)
    consumer = RedditKafkaConsumer()
    consumer.consume_messages()