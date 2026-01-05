"""
Kafka Producer OPTIMISÉ - Scrape Reddit CAN 2025
Version avec collecte intensive multi-sources
"""

import json
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests

class RedditKafkaProducer:
    def __init__(self):
        # Configuration Kafka
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'reddit-can-posts')
        self.scraping_interval = int(os.getenv('SCRAPING_INTERVAL', 300))
        
        print(f"🔌 Connexion à Kafka: {kafka_servers}")
        print(f"📍 Topic: {self.topic}")
        
        # Attendre que Kafka soit prêt
        self._wait_for_kafka(kafka_servers)
        
        # Initialiser le producer Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_request_size=10485760,
            compression_type='gzip'
        )
        
        print("✅ Connecté à Kafka!")
        
        # Session HTTP
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # ========================================
        # MOTS-CLÉS ÉLARGIS (CAN 2025)
        # ========================================
        self.keywords = [
            # CAN général
            'AFCON', 'AFCON2025', 'AFCON 2025', 'CAN 2025', 'Africa Cup',
            'African Cup of Nations', 'Africa Cup of Nations 2025',
            
            # Équipes qualifiées
            'Morocco', 'Morocco football', 'Morocco AFCON', 'Atlas Lions',
            'Senegal', 'Senegal AFCON', 'Lions of Teranga',
            'Egypt', 'Egypt AFCON', 'Pharaohs',
            'Nigeria', 'Nigeria football', 'Super Eagles',
            'Cameroon', 'Cameroon AFCON', 'Indomitable Lions',
            'Ivory Coast', 'Côte d\'Ivoire', 'Elephants',
            'Algeria', 'Algeria football', 'Desert Foxes',
            'Ghana', 'Ghana AFCON', 'Black Stars',
            'Tunisia', 'Tunisia football', 'Eagles of Carthage',
            'Mali', 'Mali football',
            'Burkina Faso', 'Burkina AFCON',
            'Guinea', 'South Africa', 'Angola', 'Mozambique',
            
            # Football africain général
            'African football', 'African soccer', 'CAF',
            'African national teams', 'African championship',
            
            # Qualifications et compétition
            'AFCON qualifiers', 'AFCON 2025 qualifiers',
            'Africa Cup qualifiers', 'African Cup qualifying'
        ]
        
        # ========================================
        # SUBREDDITS ÉLARGIS
        # ========================================
        self.subreddits = [
            # Football principal
            'soccer', 'football', 
            
            # Sports
            'sports', 'worldcup',
            
            # Afrique
            'Africa', 'africapics', 'African',
            
            # Pays africains
            'Morocco', 'Nigeria', 'Egypt', 'southafrica',
            'Algeria', 'Tunisia', 'cameroon', 'ghana',
            
            # News
            'worldnews', 'news', 'internationalsoccer'
        ]
        
        # Stratégies de tri
        self.sort_methods = ['hot', 'new', 'top', 'rising']
        
        # Statistiques
        self.stats = {
            'posts_sent': 0,
            'comments_sent': 0,
            'errors': 0,
            'duplicates': 0
        }
        
        # Set pour éviter les doublons
        self.seen_post_ids = set()
    
    def _wait_for_kafka(self, kafka_servers, timeout=60):
        """Attendre que Kafka soit prêt"""
        print("⏳ Attente de Kafka...")
        start = time.time()
        
        while time.time() - start < timeout:
            try:
                from kafka import KafkaAdminClient
                admin = KafkaAdminClient(
                    bootstrap_servers=kafka_servers,
                    request_timeout_ms=5000
                )
                admin.close()
                print("✅ Kafka est prêt!")
                return True
            except Exception as e:
                print(f"⏳ Kafka pas encore prêt... ({int(time.time() - start)}s)")
                time.sleep(5)
        
        raise Exception("❌ Timeout: Kafka n'est pas accessible")
    
    def send_to_kafka(self, data):
        """Envoie un message vers Kafka"""
        try:
            # Vérifier doublon
            if data['id'] in self.seen_post_ids:
                self.stats['duplicates'] += 1
                return False
            
            self.seen_post_ids.add(data['id'])
            
            future = self.producer.send(self.topic, value=data)
            future.get(timeout=10)
            
            if data['type'] == 'post':
                self.stats['posts_sent'] += 1
                print(f"✅ Post envoyé: {data['title'][:50]}... (score: {data['score']})")
            else:
                self.stats['comments_sent'] += 1
            
            return True
        except Exception as e:
            print(f"❌ Erreur envoi: {e}")
            self.stats['errors'] += 1
            return False
    
    def get_subreddit_posts(self, subreddit, sort='hot', limit=100, time_filter='all'):
        """Scrape les posts d'un subreddit"""
        url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
        params = {'limit': min(limit, 100)}
        
        # Pour 'top', ajouter le filtre temporel
        if sort == 'top':
            params['t'] = time_filter  # 'day', 'week', 'month', 'year', 'all'
        
        try:
            print(f"🔍 Scraping r/{subreddit}/{sort} (limit={limit})...")
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 429:
                print("⚠️  Rate limited. Waiting 60s...")
                time.sleep(60)
                return []
            
            response.raise_for_status()
            data = response.json()
            
            posts = []
            for child in data['data']['children']:
                post = self._extract_post(child['data'])
                
                # Filtrer les posts CAN
                if self._is_can_related(post):
                    posts.append(post)
                    self.send_to_kafka(post)
                    time.sleep(0.1)  # Petit délai pour ne pas surcharger
            
            print(f"   → {len(posts)} posts CAN trouvés")
            return posts
            
        except Exception as e:
            print(f"❌ Erreur scraping r/{subreddit}: {e}")
            return []
    
    def search_reddit(self, query, subreddit=None, limit=100):
        """Recherche Reddit avec un mot-clé spécifique"""
        if subreddit:
            url = f"https://www.reddit.com/r/{subreddit}/search.json"
        else:
            url = "https://www.reddit.com/search.json"
        
        params = {
            'q': query,
            'limit': min(limit, 100),
            'sort': 'relevance',
            'restrict_sr': 'on' if subreddit else 'off',
            't': 'month'  # Posts du dernier mois
        }
        
        try:
            print(f"🔎 Recherche: '{query}' {'dans r/' + subreddit if subreddit else '(global)'}...")
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 429:
                time.sleep(60)
                return []
            
            response.raise_for_status()
            data = response.json()
            
            posts = []
            for child in data['data']['children']:
                post = self._extract_post(child['data'])
                posts.append(post)
                self.send_to_kafka(post)
                time.sleep(0.1)
            
            print(f"   → {len(posts)} posts trouvés")
            return posts
            
        except Exception as e:
            print(f"❌ Erreur recherche: {e}")
            return []
    
    def get_post_comments(self, subreddit, post_id, limit=50):
        """Récupère les commentaires d'un post"""
        url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
        params = {'limit': limit}
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 429:
                time.sleep(60)
                return []
            
            response.raise_for_status()
            data = response.json()
            
            comments = []
            if len(data) > 1:
                comment_data = data[1]['data']['children']
                for child in comment_data:
                    if child['kind'] == 't1':
                        comment = self._extract_comment(child['data'], post_id)
                        if comment:
                            comments.append(comment)
                            self.send_to_kafka(comment)
            
            return comments
            
        except Exception as e:
            print(f"❌ Erreur commentaires: {e}")
            return []
    
    def _is_can_related(self, post):
        """Vérifie si le post est lié à la CAN (plus permissif)"""
        title = post.get('title', '').lower()
        text = post.get('selftext', '').lower()
        combined = title + ' ' + text
        
        # Vérifier les mots-clés
        for kw in self.keywords:
            if kw.lower() in combined:
                return True
        
        return False
    
    def _extract_post(self, post_data):
        """Extrait les données d'un post"""
        return {
            'type': 'post',
            'id': post_data.get('id'),
            'title': post_data.get('title'),
            'author': post_data.get('author'),
            'subreddit': post_data.get('subreddit'),
            'score': post_data.get('score', 0),
            'upvote_ratio': post_data.get('upvote_ratio', 0),
            'num_comments': post_data.get('num_comments', 0),
            'created_utc': post_data.get('created_utc'),
            'created_date': datetime.fromtimestamp(
                post_data.get('created_utc', 0)
            ).isoformat(),
            'selftext': post_data.get('selftext', ''),
            'url': post_data.get('url'),
            'permalink': f"https://reddit.com{post_data.get('permalink', '')}",
            'link_flair_text': post_data.get('link_flair_text'),
            'scraped_at': datetime.now().isoformat()
        }
    
    def _extract_comment(self, comment_data, post_id):
        """Extrait les données d'un commentaire"""
        if comment_data.get('body') in ['[deleted]', '[removed]']:
            return None
        
        return {
            'type': 'comment',
            'id': comment_data.get('id'),
            'post_id': post_id,
            'author': comment_data.get('author'),
            'body': comment_data.get('body'),
            'score': comment_data.get('score', 0),
            'created_utc': comment_data.get('created_utc'),
            'created_date': datetime.fromtimestamp(
                comment_data.get('created_utc', 0)
            ).isoformat(),
            'parent_id': comment_data.get('parent_id'),
            'scraped_at': datetime.now().isoformat()
        }
    
    def run_intensive_scraping(self):
        """STRATÉGIE INTENSIVE - Collecte maximale"""
        print("\n" + "="*70)
        print(f"🚀 COLLECTE INTENSIVE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        all_posts = []
        
        # ========================================
        # PHASE 1: Scraping par RECHERCHE (le plus efficace)
        # ========================================
        print("\n📍 PHASE 1: Recherche ciblée par mots-clés")
        print("-"*70)
        
        key_queries = [
            'AFCON 2025',
            'Africa Cup 2025',
            'Morocco AFCON',
            'Senegal AFCON',
            'Nigeria AFCON'
        ]
        
        for query in key_queries:
            posts = self.search_reddit(query, limit=50)
            all_posts.extend(posts)
            time.sleep(3)  # Respecter le rate limit
        
        # ========================================
        # PHASE 2: Scraping par SUBREDDIT
        # ========================================
        print("\n📍 PHASE 2: Scraping des subreddits")
        print("-"*70)
        
        for subreddit in self.subreddits[:8]:  # Limiter à 8 pour ne pas trop attendre
            for sort in ['hot', 'new']:
                posts = self.get_subreddit_posts(subreddit, sort=sort, limit=50)
                all_posts.extend(posts)
                time.sleep(2)
        
        # ========================================
        # PHASE 3: Top posts du mois (pour historique)
        # ========================================
        print("\n📍 PHASE 3: Top posts du mois")
        print("-"*70)
        
        for subreddit in ['soccer', 'Africa', 'Morocco']:
            posts = self.get_subreddit_posts(
                subreddit, 
                sort='top', 
                limit=50,
                time_filter='month'
            )
            all_posts.extend(posts)
            time.sleep(2)
        
        # ========================================
        # PHASE 4: Commentaires des top posts
        # ========================================
        print("\n💬 PHASE 4: Récupération des commentaires")
        print("-"*70)
        
        # Trier par score et prendre les 10 meilleurs
        top_posts = sorted(all_posts, key=lambda x: x.get('score', 0), reverse=True)[:10]
        
        for post in top_posts:
            print(f"   📝 Commentaires pour: {post['title'][:50]}...")
            comments = self.get_post_comments(
                post['subreddit'],
                post['id'],
                limit=30
            )
            print(f"      → {len(comments)} commentaires récupérés")
            time.sleep(2)
        
        # ========================================
        # RAPPORT FINAL
        # ========================================
        print("\n" + "="*70)
        print("📊 RAPPORT DE COLLECTE INTENSIVE")
        print("="*70)
        print(f"Posts envoyés:         {self.stats['posts_sent']}")
        print(f"Commentaires envoyés:  {self.stats['comments_sent']}")
        print(f"Doublons évités:       {self.stats['duplicates']}")
        print(f"Erreurs:               {self.stats['errors']}")
        print(f"Total unique:          {len(self.seen_post_ids)}")
        print("="*70)
    
    def run_continuous(self):
        """Exécute le scraping en continu"""
        print("="*70)
        print("🎯 REDDIT KAFKA PRODUCER - MODE INTENSIF")
        print("="*70)
        print(f"📍 Topic Kafka: {self.topic}")
        print(f"⏱️  Intervalle: {self.scraping_interval}s")
        print(f"🔍 Mots-clés: {len(self.keywords)}")
        print(f"📍 Subreddits: {len(self.subreddits)}")
        print("="*70)
        
        cycle = 1
        
        while True:
            try:
                print(f"\n\n🔄 CYCLE #{cycle}")
                self.run_intensive_scraping()
                
                print(f"\n⏸️  Pause de {self.scraping_interval}s avant prochain cycle...")
                time.sleep(self.scraping_interval)
                
                cycle += 1
                
            except KeyboardInterrupt:
                print("\n\n🛑 Arrêt demandé")
                break
            except Exception as e:
                print(f"\n❌ Erreur globale: {e}")
                self.stats['errors'] += 1
                time.sleep(60)
        
        self.close()
    
    def close(self):
        """Ferme proprement le producer"""
        print("\n🔌 Fermeture du producer...")
        self.producer.flush()
        self.producer.close()
        print("✅ Producer fermé")

if __name__ == "__main__":
    producer = RedditKafkaProducer()
    producer.run_continuous()