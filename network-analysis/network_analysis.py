"""
========================================================
🕸️ SOCIAL NETWORK ANALYSIS - Reddit CAN 2025
========================================================
✔ Construction du graphe d'interactions
✔ Analyse de centralité (degree, betweenness, closeness)
✔ Détection de communautés (Louvain)
✔ Identification des influenceurs
✔ Sauvegarde dans MongoDB
========================================================
"""

import networkx as nx
from pymongo import MongoClient
from datetime import datetime
from collections import defaultdict, Counter
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SocialNetworkAnalyzer:
    def __init__(self, mongodb_uri='mongodb://admin:admin123@mongodb:27017/'):
        """Initialise l'analyseur de réseau social"""
        logging.info("🔌 Connexion à MongoDB...")
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['reddit_can']
        
        # Collections
        self.posts_col = self.db['posts']
        self.comments_col = self.db['comments']
        self.network_col = self.db['network_analysis']
        
        # Graphes
        self.G = nx.Graph()  # Graphe non orienté
        self.DG = nx.DiGraph()  # Graphe orienté (pour influence)
        
        logging.info("✅ MongoDB connecté!")
    
    def build_interaction_graph(self):
        """
        Construit le graphe des interactions entre utilisateurs
        Relations : post-comment, comment-comment
        """
        logging.info("\n🏗️ Construction du graphe d'interactions...")
        
        interactions = defaultdict(lambda: {'weight': 0, 'types': []})
        
        # === INTERACTION 1: Auteur Post ← Auteur Comment ===
        logging.info("   📝 Analyse des posts...")
        posts_count = 0
        
        for post in self.posts_col.find({}, {'id': 1, 'author': 1}):
            post_id = post.get('id')
            post_author = post.get('author')
            
            if not post_author or post_author in ['[deleted]', 'AutoModerator']:
                continue
            
            # Trouver tous les commentaires sur ce post
            comments = self.comments_col.find({'post_id': post_id})
            
            for comment in comments:
                comment_author = comment.get('author')
                
                if not comment_author or comment_author in ['[deleted]', 'AutoModerator']:
                    continue
                
                if post_author != comment_author:
                    # Créer une arête bidirectionnelle
                    edge = tuple(sorted([post_author, comment_author]))
                    interactions[edge]['weight'] += 1
                    interactions[edge]['types'].append('post_comment')
                    
                    # Graphe orienté : comment_author → post_author
                    self.DG.add_edge(comment_author, post_author, type='reply')
            
            posts_count += 1
        
        logging.info(f"   ✅ {posts_count} posts analysés")
        
        # === INTERACTION 2: Commentaires entre eux (threads) ===
        logging.info("   💬 Analyse des threads de commentaires...")
        
        comment_authors = {}
        for comment in self.comments_col.find({}, {'id': 1, 'author': 1, 'parent_id': 1}):
            comment_id = comment.get('id')
            author = comment.get('author')
            parent_id = comment.get('parent_id', '').replace('t1_', '')  # t1_ = commentaire
            
            if author and author not in ['[deleted]', 'AutoModerator']:
                comment_authors[comment_id] = author
                
                # Si réponse à un autre commentaire
                if parent_id in comment_authors:
                    parent_author = comment_authors[parent_id]
                    
                    if author != parent_author:
                        edge = tuple(sorted([author, parent_author]))
                        interactions[edge]['weight'] += 2  # Poids plus fort (interaction directe)
                        interactions[edge]['types'].append('comment_reply')
                        
                        # Graphe orienté
                        self.DG.add_edge(author, parent_author, type='comment_reply')
        
        # === Construire le graphe non orienté ===
        for (user1, user2), data in interactions.items():
            self.G.add_edge(
                user1, user2,
                weight=data['weight'],
                types=list(set(data['types']))
            )
        
        logging.info(f"\n📊 Graphe construit:")
        logging.info(f"   👥 Nœuds (utilisateurs): {self.G.number_of_nodes()}")
        logging.info(f"   🔗 Arêtes (interactions): {self.G.number_of_edges()}")
        logging.info(f"   📈 Densité: {nx.density(self.G):.4f}")
        
        return self.G
    
    def compute_centralities(self):
        """
        Calcule les différentes mesures de centralité
        """
        logging.info("\n📐 Calcul des centralités...")
        
        if self.G.number_of_nodes() == 0:
            logging.warning("⚠️ Graphe vide, impossible de calculer les centralités")
            return {}
        
        centralities = {}
        
        # === 1. DEGREE CENTRALITY ===
        # Mesure la popularité (nombre de connexions)
        logging.info("   🔢 Degree centrality...")
        degree_cent = nx.degree_centrality(self.G)
        centralities['degree'] = degree_cent
        
        # === 2. BETWEENNESS CENTRALITY ===
        # Mesure le rôle de "pont" entre communautés
        logging.info("   🌉 Betweenness centrality...")
        try:
            betweenness_cent = nx.betweenness_centrality(self.G, weight='weight')
            centralities['betweenness'] = betweenness_cent
        except:
            centralities['betweenness'] = {}
        
        # === 3. CLOSENESS CENTRALITY ===
        # Mesure la proximité au reste du réseau
        logging.info("   📏 Closeness centrality...")
        try:
            closeness_cent = nx.closeness_centrality(self.G, distance='weight')
            centralities['closeness'] = closeness_cent
        except:
            centralities['closeness'] = {}
        
        # === 4. EIGENVECTOR CENTRALITY ===
        # Mesure l'influence (connecté à des personnes influentes)
        logging.info("   🎯 Eigenvector centrality...")
        try:
            eigen_cent = nx.eigenvector_centrality(self.G, max_iter=1000, weight='weight')
            centralities['eigenvector'] = eigen_cent
        except:
            centralities['eigenvector'] = {}
        
        # === 5. PAGE RANK (sur graphe orienté) ===
        logging.info("   🏆 PageRank...")
        try:
            pagerank = nx.pagerank(self.DG, weight='weight')
            centralities['pagerank'] = pagerank
        except:
            centralities['pagerank'] = {}
        
        logging.info("   ✅ Centralités calculées!")
        
        return centralities
    
    def detect_communities(self):
        """
        Détecte les communautés dans le réseau
        Utilise l'algorithme de Louvain
        """
        logging.info("\n🔍 Détection des communautés...")
        
        if self.G.number_of_nodes() == 0:
            logging.warning("⚠️ Graphe vide")
            return {}
        
        try:
            # Louvain (meilleure modularité)
            from networkx.algorithms import community
            communities = community.louvain_communities(self.G, weight='weight', seed=42)
            
            # Convertir en dict user → community_id
            user_community = {}
            for i, comm in enumerate(communities):
                for user in comm:
                    user_community[user] = i
            
            logging.info(f"   ✅ {len(communities)} communautés détectées")
            
            # Statistiques par communauté
            for i, comm in enumerate(communities):
                logging.info(f"      Community {i}: {len(comm)} membres")
            
            return user_community
        
        except Exception as e:
            logging.error(f"   ❌ Erreur détection: {e}")
            return {}
    
    def identify_influencers(self, centralities, top_n=20):
        """
        Identifie les utilisateurs les plus influents
        Combine plusieurs métriques
        """
        logging.info(f"\n🌟 Identification des {top_n} influenceurs...")
        
        # Score composite
        influencer_scores = defaultdict(float)
        
        # Pondération des différentes métriques
        weights = {
            'degree': 0.25,
            'betweenness': 0.20,
            'eigenvector': 0.25,
            'pagerank': 0.30
        }
        
        for metric, weight in weights.items():
            if metric in centralities and centralities[metric]:
                # Normaliser les scores entre 0 et 1
                values = list(centralities[metric].values())
                if values:
                    max_val = max(values)
                    min_val = min(values)
                    range_val = max_val - min_val if max_val != min_val else 1
                    
                    for user, score in centralities[metric].items():
                        normalized = (score - min_val) / range_val
                        influencer_scores[user] += normalized * weight
        
        # Top influenceurs
        top_influencers = sorted(
            influencer_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
        
        logging.info(f"   ✅ Top {top_n} influenceurs identifiés")
        
        return top_influencers
    
    def get_user_activity(self):
        """
        Récupère les statistiques d'activité par utilisateur
        """
        logging.info("\n📊 Calcul des statistiques d'activité...")
        
        user_stats = defaultdict(lambda: {
            'posts': 0,
            'comments': 0,
            'total_score': 0,
            'avg_score': 0,
            'total_interactions': 0
        })
        
        # Posts
        for post in self.posts_col.find({}, {'author': 1, 'score': 1}):
            author = post.get('author')
            if author and author not in ['[deleted]', 'AutoModerator']:
                user_stats[author]['posts'] += 1
                user_stats[author]['total_score'] += post.get('score', 0)
        
        # Commentaires
        for comment in self.comments_col.find({}, {'author': 1, 'score': 1}):
            author = comment.get('author')
            if author and author not in ['[deleted]', 'AutoModerator']:
                user_stats[author]['comments'] += 1
                user_stats[author]['total_score'] += comment.get('score', 0)
        
        # Calculer moyennes et interactions
        for user, stats in user_stats.items():
            total_activity = stats['posts'] + stats['comments']
            stats['avg_score'] = stats['total_score'] / total_activity if total_activity > 0 else 0
            stats['total_interactions'] = total_activity
        
        logging.info(f"   ✅ Statistiques pour {len(user_stats)} utilisateurs")
        
        return user_stats
    
    def save_to_mongodb(self, centralities, communities, influencers, user_stats):
        """
        Sauvegarde les résultats dans MongoDB
        """
        logging.info("\n💾 Sauvegarde dans MongoDB...")
        
        documents = []
        
        for user in self.G.nodes():
            doc = {
                'user': user,
                'centralities': {
                    'degree': centralities.get('degree', {}).get(user, 0),
                    'betweenness': centralities.get('betweenness', {}).get(user, 0),
                    'closeness': centralities.get('closeness', {}).get(user, 0),
                    'eigenvector': centralities.get('eigenvector', {}).get(user, 0),
                    'pagerank': centralities.get('pagerank', {}).get(user, 0)
                },
                'community_id': communities.get(user, -1),
                'is_influencer': user in [u for u, _ in influencers],
                'influencer_rank': next((i+1 for i, (u, _) in enumerate(influencers) if u == user), None),
                'activity': user_stats.get(user, {}),
                'degree': self.G.degree(user),
                'weighted_degree': sum([d['weight'] for _, _, d in self.G.edges(user, data=True)]),
                'analyzed_at': datetime.now().isoformat()
            }
            documents.append(doc)
        
        # Sauvegarder
        if documents:
            self.network_col.delete_many({})  # Clear old data
            self.network_col.insert_many(documents)
            logging.info(f"   ✅ {len(documents)} documents sauvegardés")
        
        # Sauvegarder aussi les métadonnées du graphe
        metadata = {
            'type': 'graph_metadata',
            'num_nodes': self.G.number_of_nodes(),
            'num_edges': self.G.number_of_edges(),
            'density': nx.density(self.G),
            'num_communities': len(set(communities.values())),
            'average_clustering': nx.average_clustering(self.G),
            'analyzed_at': datetime.now().isoformat()
        }
        
        self.db['network_metadata'].update_one(
            {'type': 'graph_metadata'},
            {'$set': metadata},
            upsert=True
        )
        
        logging.info("   ✅ Métadonnées sauvegardées")
    
    def run_complete_analysis(self):
        """
        Lance l'analyse complète
        """
        logging.info("\n" + "="*70)
        logging.info("🕸️ DÉMARRAGE DE L'ANALYSE DE RÉSEAU SOCIAL")
        logging.info("="*70)
        
        # 1. Construire le graphe
        self.build_interaction_graph()
        
        if self.G.number_of_nodes() == 0:
            logging.error("❌ Aucune interaction trouvée. Arrêt.")
            return
        
        # 2. Calculer centralités
        centralities = self.compute_centralities()
        
        # 3. Détecter communautés
        communities = self.detect_communities()
        
        # 4. Identifier influenceurs
        influencers = self.identify_influencers(centralities, top_n=20)
        
        # 5. Statistiques utilisateurs
        user_stats = self.get_user_activity()
        
        # 6. Sauvegarder
        self.save_to_mongodb(centralities, communities, influencers, user_stats)
        
        # === RAPPORT FINAL ===
        logging.info("\n" + "="*70)
        logging.info("📊 RAPPORT FINAL - ANALYSE DE RÉSEAU")
        logging.info("="*70)
        
        logging.info(f"\n🌐 Graphe Global:")
        logging.info(f"   Utilisateurs (nœuds): {self.G.number_of_nodes()}")
        logging.info(f"   Interactions (arêtes): {self.G.number_of_edges()}")
        logging.info(f"   Densité: {nx.density(self.G):.4f}")
        logging.info(f"   Clustering moyen: {nx.average_clustering(self.G):.4f}")
        
        logging.info(f"\n🏘️ Communautés:")
        logging.info(f"   Nombre de communautés: {len(set(communities.values()))}")
        
        logging.info(f"\n🌟 Top 10 Influenceurs:")
        for i, (user, score) in enumerate(influencers[:10], 1):
            activity = user_stats.get(user, {})
            logging.info(f"   {i:2d}. {user:20s} | Score: {score:.4f} | "
                        f"Posts: {activity.get('posts', 0):3d} | "
                        f"Comments: {activity.get('comments', 0):3d}")
        
        logging.info("\n" + "="*70)
        logging.info("✅ ANALYSE TERMINÉE AVEC SUCCÈS")
        logging.info("="*70)
        
        self.close()
    
    def close(self):
        """Ferme la connexion MongoDB"""
        self.client.close()
        logging.info("🔌 Connexion MongoDB fermée")


if __name__ == "__main__":
    analyzer = SocialNetworkAnalyzer()
    analyzer.run_complete_analysis()