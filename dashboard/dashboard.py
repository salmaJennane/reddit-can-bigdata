"""
========================================================
📊 DASHBOARD PRO - Reddit CAN 2025 Analytics
========================================================
✅ PARTIE 1/2 - CONFIGURATION + PAGES 1-3
✔ Titres VISIBLES
✔ Clustering correct
✔ Tous graphiques fonctionnels
========================================================
"""

import streamlit as st
import pymongo
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from collections import Counter
import networkx as nx
import re

# ===============================
# CONFIGURATION PAGE
# ===============================
st.set_page_config(
    page_title="Reddit CAN 2025 | Big Data Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===============================
# CSS PROFESSIONNEL
# ===============================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        text-align: center;
        color: #667eea !important;
        padding: 2rem 0 1rem 0;
        margin-bottom: 2rem;
        animation: fadeIn 1s ease-in;
    }
    
    .sub-header {
        text-align: center;
        color: #64748b !important;
        font-size: 1.2rem;
        margin-top: -1.5rem;
        margin-bottom: 2rem;
    }
    
    .section-header {
        font-size: 2rem;
        font-weight: 600;
        color: #1e293b !important;
        margin-top: 3rem;
        margin-bottom: 1.5rem;
        padding-bottom: 0.8rem;
        border-bottom: 4px solid #667eea;
        position: relative;
    }
    
    .section-header::before {
        content: '';
        position: absolute;
        bottom: -4px;
        left: 0;
        width: 80px;
        height: 4px;
        background: #764ba2;
    }
    
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e293b 0%, #334155 100%);
    }
    
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        padding: 1.5rem;
        border-radius: 12px;
        border: 2px solid #e2e8f0;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.05);
    }
    
    [data-testid="stMetric"] label {
        font-weight: 600 !important;
        font-size: 0.95rem !important;
        color: #64748b !important;
    }
    
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        color: #1e293b !important;
    }
    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(-20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ===============================
# CONNEXION MONGODB
# ===============================
@st.cache_resource
def get_mongo_client():
    try:
        client = MongoClient(
            'mongodb://admin:admin123@mongodb:27017/',
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"❌ Erreur MongoDB: {e}")
        return None

# ===============================
# CHARGEMENT DONNÉES
# ===============================
@st.cache_data(ttl=30)
def load_all_data():
    client = get_mongo_client()
    if not client:
        return None
    
    db = client['reddit_can']
    
    data = {
        'posts': pd.DataFrame(list(db['posts'].find({}, {'_id': 0}))),
        'comments': pd.DataFrame(list(db['comments'].find({}, {'_id': 0}))),
        'processed': pd.DataFrame(list(db['processed_posts'].find({}, {'_id': 0}))),
        'sentiments': pd.DataFrame(list(db['sentiment_results'].find({}, {'_id': 0}))),
        'network': pd.DataFrame(list(db['network_analysis'].find({}, {'_id': 0}))),
        'metadata': db['network_metadata'].find_one({'type': 'graph_metadata'}, {'_id': 0})
    }
    
    for key in ['posts', 'comments', 'processed']:
        if not data[key].empty and 'created_date' in data[key].columns:
            data[key]['created_date'] = pd.to_datetime(data[key]['created_date'], errors='coerce')
    
    if not data['sentiments'].empty and 'analyzed_at' in data['sentiments'].columns:
        data['sentiments']['analyzed_at'] = pd.to_datetime(data['sentiments']['analyzed_at'], errors='coerce')
    
    return data

# ===============================
# SIDEBAR NAVIGATION
# ===============================
st.sidebar.markdown("# 🎛️ Navigation")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "",
    [
        "🏠 Accueil",
        "🕸️ Réseau Social",
        "💭 Sentiments",
        "📝 Posts & Topics",
        "📊 Statistiques"
    ],
    label_visibility="collapsed"
)

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Paramètres")

auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)", value=False)
if auto_refresh:
    st.sidebar.success("✅ Actif")

# ===============================
# CHARGEMENT DONNÉES
# ===============================
with st.spinner("📥 Chargement des données..."):
    data = load_all_data()

if data is None:
    st.error("❌ Impossible de charger les données")
    st.stop()

df_posts = data['posts']
df_comments = data['comments']
df_processed = data['processed']
df_sentiments = data['sentiments']
df_network = data['network']
metadata = data['metadata']

# ===============================
# PAGE 1: ACCUEIL
# ===============================
if page == "🏠 Accueil":
    
    st.markdown('<h1 class="main-header">⚽ Reddit CAN 2025 - Big Data Analytics</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Pipeline temps réel | Kafka • Spark • MongoDB • ML</p>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="📝 Posts",
            value=f"{len(df_posts):,}",
            delta=f"+{len(df_posts) - len(df_processed)}" if len(df_posts) > len(df_processed) else None
        )
    
    with col2:
        st.metric(
            label="💬 Commentaires",
            value=f"{len(df_comments):,}"
        )
    
    with col3:
        st.metric(
            label="👥 Utilisateurs",
            value=f"{len(df_network):,}" if not df_network.empty else "0"
        )
    
    with col4:
        st.metric(
            label="🤖 ML Analyzed",
            value=f"{len(df_sentiments):,}"
        )
    
    with col5:
        engagement = df_posts['score'].sum() if not df_posts.empty and 'score' in df_posts.columns else 0
        st.metric(
            label="📈 Engagement",
            value=f"{engagement:,}"
        )
    
    st.markdown("---")
    
    st.markdown('<div class="section-header">📊 Vue d\'Ensemble</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 Évolution Temporelle")
        if not df_posts.empty and 'created_date' in df_posts.columns:
            df_posts['date'] = df_posts['created_date'].dt.date
            timeline = df_posts.groupby('date').size().reset_index(name='count')
            
            fig = px.area(
                timeline,
                x='date',
                y='count',
                labels={'date': 'Date', 'count': 'Posts'},
                color_discrete_sequence=['#667eea']
            )
            fig.update_traces(fillcolor='rgba(102, 126, 234, 0.2)', line=dict(width=3))
            fig.update_layout(height=350, hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### 🔝 Top Subreddits")
        if not df_posts.empty and 'subreddit' in df_posts.columns:
            top_subs = df_posts['subreddit'].value_counts().head(8)
            
            fig = px.bar(
                x=top_subs.values,
                y=top_subs.index,
                orientation='h',
                labels={'x': 'Posts', 'y': 'Subreddit'},
                color=top_subs.values,
                color_continuous_scale='Purples'
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


# ===============================
# PAGE 2: RÉSEAU SOCIAL (OPTIMISÉE ✅)
# ===============================
elif page == "🕸️ Réseau Social":
    
    st.markdown('<h1 class="main-header">🕸️ Analyse du Réseau Social</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Qui sont les influenceurs ? Quelles sont les communautés ?</p>', unsafe_allow_html=True)
    
    if df_network.empty:
        st.warning("⚠️ Aucune donnée de réseau. Lancez l'analyse réseau d'abord.")
        st.code("docker-compose run --rm network-analysis")
        st.stop()
    
    if metadata:
        st.markdown("### 📊 Vue d'Ensemble du Réseau")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("👥 Utilisateurs", f"{metadata.get('num_nodes', 0):,}")
        
        with col2:
            st.metric("🔗 Interactions", f"{metadata.get('num_edges', 0):,}")
        
        with col3:
            density = metadata.get('density', 0) * 100
            st.metric("📊 Densité", f"{density:.2f}%")
            st.caption("% connexions possibles")
        
        with col4:
            st.metric("🏘️ Communautés", metadata.get('num_communities', 0))
            st.caption("Groupes détectés")
        
        with col5:
            # Calcul du clustering
            clustering = metadata.get('average_clustering', 0)
            if clustering == 0 and 'clustering_coefficient' in df_network.columns:
                clustering = df_network['clustering_coefficient'].mean()
            st.metric("🎯 Clustering", f"{clustering*100:.1f}%")
            st.caption("Tendance groupes")
    
    st.markdown("---")
    
    # ==========================================
    # TOP 10 INFLUENCEURS - COMPARAISON VISUELLE
    # ==========================================
    st.markdown('<div class="section-header">🌟 Top 10 Influenceurs</div>', unsafe_allow_html=True)
    
    top10 = df_network[df_network['is_influencer'] == True].sort_values('influencer_rank').head(10)
    
    if not top10.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🏆 Score Total d'Engagement")
            st.caption("Somme des upvotes de tous les posts/commentaires")
            
            # Extraire les scores d'engagement
            engagement_scores = top10['activity'].apply(
                lambda x: x.get('total_score', 0) if isinstance(x, dict) else 0
            )
            
            fig = px.bar(
                x=engagement_scores.values,
                y=top10['user'].values,
                orientation='h',
                labels={'x': 'Score Total (upvotes)', 'y': 'Utilisateur'},
                color=engagement_scores.values,
                color_continuous_scale='Reds',
                text=engagement_scores.values
            )
            fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            fig.update_layout(height=450, showlegend=False, yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 🔗 Nombre d'Interactions")
            st.caption("Nombre d'utilisateurs différents avec qui il/elle interagit")
            
            degrees = top10['degree'].values
            
            fig = px.bar(
                x=degrees,
                y=top10['user'].values,
                orientation='h',
                labels={'x': 'Nombre d\'interactions', 'y': 'Utilisateur'},
                color=degrees,
                color_continuous_scale='Blues',
                text=degrees
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(height=450, showlegend=False, yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ==========================================
    # DÉTAILS DES TOP 15 INFLUENCEURS
    # ==========================================
    st.markdown('<div class="section-header">📋 Profils Détaillés des Influenceurs</div>', unsafe_allow_html=True)
    
    influencers = df_network[df_network['is_influencer'] == True].sort_values('influencer_rank').head(15)
    
    if not influencers.empty:
        for i, row in influencers.iterrows():
            with st.expander(f"🏆 **#{int(row['influencer_rank'])} - {row['user']}**"):
                
                activity = row.get('activity', {})
                centralities = row.get('centralities', {})
                
                # Métriques en colonnes
                col1, col2, col3, col4 = st.columns(4)
                
                col1.metric("📝 Posts", activity.get('posts', 0))
                col2.metric("💬 Comments", activity.get('comments', 0))
                col3.metric("⭐ Score Total", f"{activity.get('total_score', 0):,}")
                col4.metric("🏘️ Communauté", f"#{row.get('community_id', 'N/A')}")
                
                st.markdown("---")
                
                # Graphique des métriques de centralité
                st.markdown("#### 📈 Métriques d'Influence")
                
                metrics_data = {
                    'Métrique': ['Degree', 'Betweenness', 'Closeness', 'Eigenvector'],
                    'Score': [
                        centralities.get('degree', 0) * 100,
                        centralities.get('betweenness', 0) * 100,
                        centralities.get('closeness', 0) * 100,
                        centralities.get('eigenvector', 0) * 100
                    ]
                }
                
                fig = px.bar(
                    metrics_data,
                    x='Score',
                    y='Métrique',
                    orientation='h',
                    color='Score',
                    color_continuous_scale='Viridis',
                    text='Score'
                )
                fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                fig.update_layout(height=250, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
                
                # Explications
                st.caption(f"""
                💡 **Interprétation** :
                - **Degree ({int(row.get('degree', 0))})** : Nombre de personnes avec qui il/elle interagit directement
                - **Betweenness** : Mesure son rôle de "pont" entre différents groupes d'utilisateurs
                - **Closeness** : Indique sa proximité moyenne avec tous les autres utilisateurs du réseau
                - **Eigenvector** : Mesure l'influence de ses connexions (connecté à d'autres influenceurs)
                """)
    
    st.markdown("---")
    
    # ==========================================
    # COMMUNAUTÉS - VERSION OPTIMISÉE ✅
    # ==========================================
    st.markdown('<div class="section-header">🏘️ Communautés Détectées</div>', unsafe_allow_html=True)
    
    st.markdown("""
    **Algorithme de Louvain** : Détecte automatiquement les groupes d'utilisateurs qui interagissent fréquemment ensemble.
    
    💡 **Interprétation** : 
    - Grandes communautés (>25 membres) = Groupes de fans très actifs
    - Moyennes (15-25 membres) = Discussions thématiques
    - Petites (<15 membres) = Niches spécialisées
    """)
    
    if 'community_id' in df_network.columns:
        # Calcul des statistiques par communauté
        community_stats = df_network.groupby('community_id').agg({
            'user': 'count',
            'degree': 'mean',
            'activity': lambda x: sum([a.get('total_score', 0) for a in x if isinstance(a, dict)])
        }).reset_index()
        
        community_stats.columns = ['community_id', 'membres', 'degree_moyen', 'engagement_total']
        community_stats = community_stats.sort_values('membres', ascending=False).head(10)
        
        # ✅ Nommer les communautés de façon intelligente
        def name_community(cid, size, engagement):
            if size >= 25:
                tier = "Grande"
                emoji = "🌟"
            elif size >= 15:
                tier = "Moyenne"
                emoji = "📊"
            else:
                tier = "Petite"
                emoji = "💬"
            
            return f"{emoji} Communauté #{int(cid)} ({tier})"
        
        community_stats['nom'] = community_stats.apply(
            lambda row: name_community(row['community_id'], row['membres'], row['engagement_total']),
            axis=1
        )
        
        # ✅ GRAPHIQUES OPTIMISÉS
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 👥 Taille des Communautés (Top 10)")
            
            fig = px.bar(
                community_stats.sort_values('membres', ascending=True),  # Trier pour affichage
                x='membres',
                y='nom',
                orientation='h',
                labels={'nom': 'Communauté', 'membres': 'Nombre de Membres'},
                color='membres',
                color_continuous_scale='Greens',
                text='membres'
            )
            fig.update_traces(textposition='outside', texttemplate='%{text} membres')
            fig.update_layout(
                height=450, 
                showlegend=False,
                xaxis_title="Nombre de Membres",
                yaxis_title=""
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 📈 Engagement Total par Communauté")
            
            fig = px.bar(
                community_stats.sort_values('engagement_total', ascending=True),
                x='engagement_total',
                y='nom',
                orientation='h',
                labels={'nom': 'Communauté', 'engagement_total': 'Engagement Total (upvotes)'},
                color='engagement_total',
                color_continuous_scale='Oranges',
                text='engagement_total'
            )
            fig.update_traces(textposition='outside', texttemplate='%{text:,.0f}')
            fig.update_layout(
                height=450, 
                showlegend=False,
                xaxis_title="Engagement Total (upvotes)",
                yaxis_title=""
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # ✅ TABLEAU DÉTAILLÉ
        st.markdown("#### 📋 Tableau Récapitulatif des Communautés")
        
        display_df = community_stats[['nom', 'membres', 'degree_moyen', 'engagement_total']].copy()
        display_df['degree_moyen'] = display_df['degree_moyen'].round(2)
        display_df['engagement_total'] = display_df['engagement_total'].astype(int)
        display_df.columns = ['Communauté', 'Membres', 'Interactions Moy.', 'Engagement Total']
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Engagement Total": st.column_config.NumberColumn(
                    "Engagement Total",
                    format="%d 👍"
                ),
                "Interactions Moy.": st.column_config.NumberColumn(
                    "Interactions Moy.",
                    format="%.2f"
                )
            }
        )
        
        # ✅ ANALYSE TEXTUELLE
        st.markdown("---")
        st.markdown("#### 💡 Insights Clés sur les Communautés")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            biggest_community = community_stats.iloc[0]
            st.success(f"""
            **🌟 Plus Grande Communauté**
            
            {biggest_community['nom']} compte **{biggest_community['membres']} membres**.
            
            C'est le groupe le plus actif du réseau !
            """)
        
        with col2:
            most_engaged = community_stats.sort_values('engagement_total', ascending=False).iloc[0]
            st.info(f"""
            **📈 Communauté la Plus Engagée**
            
            {most_engaged['nom']} génère **{most_engaged['engagement_total']:,.0f} upvotes**.
            
            Leurs posts ont le plus d'impact !
            """)
        
        with col3:
            avg_members = community_stats['membres'].mean()
            total_communities = metadata.get('num_communities', 0)
            st.warning(f"""
            **🏘️ Vue d'Ensemble**
            
            **{total_communities} communautés** au total.
            
            Taille moyenne : **{avg_members:.0f} membres**.
            """)
    else:
        st.warning("⚠️ Aucune information de communauté disponible dans les données")
    
    st.markdown("---")
    
    # ==========================================
    # INSIGHTS FINAUX DU RÉSEAU
    # ==========================================
    st.markdown('<div class="section-header">💡 Insights Clés du Réseau</div>', unsafe_allow_html=True)
    
    if metadata and not df_network.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            density_val = metadata.get('density', 0)
            st.info(f"""
            **🌐 Structure du Réseau**
            
            Densité : **{density_val*100:.2f}%**
            
            {"Le réseau est **très fragmenté** : les utilisateurs forment des petits groupes isolés plutôt qu'une grande communauté unifiée." if density_val < 0.01 else "Le réseau est **bien connecté** : les utilisateurs interagissent largement entre eux."}
            """)
        
        with col2:
            num_communities = metadata.get('num_communities', 0)
            num_users = metadata.get('num_nodes', 0)
            avg_size = num_users / num_communities if num_communities > 0 else 0
            
            st.success(f"""
            **🏘️ Segmentation des Discussions**
            
            **{num_communities} communautés** détectées
            
            Taille moyenne : **{avg_size:.0f} membres**
            
            {"Forte segmentation : les discussions sont cloisonnées par groupes d'intérêt." if num_communities > 10 else "Discussions assez unifiées : peu de segmentation."}
            """)
        
        with col3:
            if not df_network[df_network['is_influencer'] == True].empty:
                top_influencer = df_network[df_network['is_influencer'] == True].sort_values('influencer_rank').iloc[0]
                top_user = top_influencer['user']
                top_degree = int(top_influencer.get('degree', 0))
                top_score = top_influencer.get('activity', {}).get('total_score', 0)
                
                st.warning(f"""
                **🌟 Influenceur Principal**
                
                **{top_user}**
                
                - {top_degree} connexions
                - {top_score:,} upvotes
                
                Leader d'opinion clé sur la CAN 2025 !
                """)
            else:
                st.info("Aucun influenceur détecté")

# ===============================
# PAGE 3: SENTIMENTS
# ===============================
elif page == "💭 Sentiments":
    
    st.markdown('<h1 class="main-header">💭 Analyse des Sentiments</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Classification ML des opinions</p>', unsafe_allow_html=True)
    
    if df_sentiments.empty:
        st.warning("⚠️ Aucune analyse disponible.")
        st.code("docker-compose run --rm spark-ml-sentiment")
        st.stop()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🎯 Modèle", "Random Forest")
    
    with col2:
        st.metric("📊 Accuracy", "85-95%")
    
    with col3:
        st.metric("📈 Baseline", "VADER")
    
    with col4:
        coverage = (len(df_sentiments) / len(df_processed) * 100) if len(df_processed) > 0 else 0
        st.metric("✅ Coverage", f"{coverage:.1f}%")
    
    st.markdown("---")
    
    st.markdown('<div class="section-header">📊 Distribution</div>', unsafe_allow_html=True)
    
    sentiment_col = None
    for col_name in ['ml_prediction', 'vader_label', 'predicted_sentiment', 'sentiment']:
        if col_name in df_sentiments.columns:
            sentiment_col = col_name
            break
    
    if not sentiment_col:
        st.error("❌ Aucune colonne sentiment")
        st.stop()
    
    col1, col2 = st.columns(2)
    
    with col1:
        sentiment_counts = df_sentiments[sentiment_col].value_counts()
        
        colors = {
            'positive': '#10b981',
            'neutral': '#6b7280',
            'negative': '#ef4444'
        }
        
        fig = px.pie(
            values=sentiment_counts.values,
            names=sentiment_counts.index,
            color=sentiment_counts.index,
            color_discrete_map=colors,
            hole=0.5
        )
        fig.update_traces(textposition='outside', textinfo='percent+label')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            x=sentiment_counts.index,
            y=sentiment_counts.values,
            color=sentiment_counts.index,
            color_discrete_map=colors,
            text=sentiment_counts.values
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    st.markdown('<div class="section-header">📝 Exemples</div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["😊 Positifs", "😐 Neutres", "😠 Négatifs"])
    
    for tab, sentiment, emoji in [(tab1, 'positive', '✅'), (tab2, 'neutral', 'ℹ️'), (tab3, 'negative', '⚠️')]:
        with tab:
            posts = df_sentiments[df_sentiments[sentiment_col] == sentiment].head(5)
            
            if not posts.empty:
                for idx, row in posts.iterrows():
                    post_id = row.get('post_id') or row.get('id')
                    
                    if post_id and not df_posts.empty:
                        matching = df_posts[df_posts['id'] == post_id]
                        
                        if not matching.empty:
                            post = matching.iloc[0]
                            title = post.get('title', 'Sans titre')
                            
                            with st.expander(f"{emoji} {title[:70]}..."):
                                col1, col2, col3 = st.columns(3)
                                col1.metric("⭐", post.get('score', 0))
                                col2.metric("💬", post.get('num_comments', 0))
                                col3.metric("📍", f"r/{post.get('subreddit', 'N/A')}")
                                
                                text = post.get('selftext') or post.get('body', '')
                                if text and str(text) != 'nan':
                                    st.text(str(text)[:400])
            else:
                st.info(f"Aucun post {sentiment}")
# ===============================
# PAGE 4: POSTS & TOPICS
# ===============================
elif page == "📝 Posts & Topics":
    
    st.markdown('<h1 class="main-header">📝 Posts & Topics</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Exploration des discussions</p>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if not df_posts.empty and 'subreddit' in df_posts.columns:
            subreddits = ['Tous'] + sorted(df_posts['subreddit'].unique().tolist())
            selected_subreddit = st.selectbox("🔍 Subreddit", subreddits)
    
    with col2:
        if not df_posts.empty and 'score' in df_posts.columns:
            min_score = int(df_posts['score'].min())
            max_score = int(df_posts['score'].max())
            score_filter = st.slider("⭐ Score Min", min_score, max_score, min_score)
    
    with col3:
        sort_by = st.selectbox("📊 Trier par", ["Score", "Date", "Commentaires"])
    
    filtered_df = df_posts.copy()
    
    if selected_subreddit != 'Tous':
        filtered_df = filtered_df[filtered_df['subreddit'] == selected_subreddit]
    
    if 'score' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['score'] >= score_filter]
    
    if sort_by == "Score" and 'score' in filtered_df.columns:
        filtered_df = filtered_df.sort_values('score', ascending=False)
    elif sort_by == "Date" and 'created_date' in filtered_df.columns:
        filtered_df = filtered_df.sort_values('created_date', ascending=False)
    elif sort_by == "Commentaires" and 'num_comments' in filtered_df.columns:
        filtered_df = filtered_df.sort_values('num_comments', ascending=False)
    
    st.markdown(f"**{len(filtered_df):,} posts trouvés**")
    
    st.markdown('<div class="section-header">📄 Posts Détaillés</div>', unsafe_allow_html=True)
    
    for i, row in filtered_df.head(15).iterrows():
        with st.expander(f"**{row.get('title', 'Sans titre')[:80]}...**"):
            col1, col2, col3, col4 = st.columns(4)
            
            col1.metric("⭐ Score", row.get('score', 0))
            col2.metric("💬 Comments", row.get('num_comments', 0))
            col3.metric("📍 Subreddit", f"r/{row.get('subreddit', 'N/A')}")
            col4.metric("👤 Auteur", row.get('author', 'N/A'))
            
            if row.get('selftext'):
                st.markdown("**Contenu :**")
                st.text(row['selftext'][:500] + "..." if len(str(row['selftext'])) > 500 else row['selftext'])

#===============================
# PAGE 5: STATISTIQUES VISUELLES ✅
# ===============================
elif page == "📊 Statistiques":
    
    st.markdown('<h1 class="main-header">📊 Statistiques Avancées</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Analyses statistiques approfondies avec insights visuels</p>', unsafe_allow_html=True)
    
    st.markdown('<div class="section-header">📊 Analyse des Données Clés</div>', unsafe_allow_html=True)
    
    # ✅ NOUVEAU: VISUALISATIONS SIMPLES ET CLAIRES
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if not df_posts.empty and 'score' in df_posts.columns:
            st.markdown("#### 📊 Répartition des Scores")
            
            scores = df_posts['score']
            
            # Catégorisation simple
            categories = {
                '🔥 Très populaire (>100)': (scores > 100).sum(),
                '👍 Populaire (50-100)': ((scores >= 50) & (scores <= 100)).sum(),
                '😐 Moyen (10-50)': ((scores >= 10) & (scores < 50)).sum(),
                '📉 Faible (<10)': (scores < 10).sum()
            }
            
            df_cat = pd.DataFrame(
                list(categories.items()),
                columns=['Catégorie', 'Nombre']
            )
            
            fig = px.bar(
                df_cat,
                x='Nombre',
                y='Catégorie',
                orientation='h',
                color='Nombre',
                color_continuous_scale='Purples',
                text='Nombre'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            total = len(scores)
            st.info(f"**{categories['📉 Faible (<10)']/total*100:.1f}%** des posts ont un score très faible (<10 upvotes)")
    
    with col2:
        if not df_posts.empty and 'num_comments' in df_posts.columns:
            st.markdown("#### 💬 Répartition des Commentaires")
            
            comments = df_posts['num_comments']
            
            # Catégorisation simple
            categories = {
                '🔥 Très actif (>50)': (comments > 50).sum(),
                '💬 Actif (20-50)': ((comments >= 20) & (comments <= 50)).sum(),
                '🗨️ Modéré (5-20)': ((comments >= 5) & (comments < 20)).sum(),
                '🤐 Peu/Pas (0-5)': (comments < 5).sum()
            }
            
            df_cat = pd.DataFrame(
                list(categories.items()),
                columns=['Catégorie', 'Nombre']
            )
            
            fig = px.bar(
                df_cat,
                x='Nombre',
                y='Catégorie',
                orientation='h',
                color='Nombre',
                color_continuous_scale='Blues',
                text='Nombre'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            total = len(comments)
            very_active = categories['🔥 Très actif (>50)']
            st.success(f"**{very_active}** posts ({very_active/total*100:.1f}%) génèrent plus de 50 commentaires !")
    
    with col3:
        if not df_network.empty and 'degree' in df_network.columns:
            st.markdown("#### 🔗 Répartition des Connexions")
            
            degrees = df_network['degree']
            
            # Catégorisation simple
            categories = {
                '🌟 Super-connecté (>10)': (degrees > 10).sum(),
                '👥 Bien connecté (5-10)': ((degrees >= 5) & (degrees <= 10)).sum(),
                '🤝 Connecté (2-5)': ((degrees >= 2) & (degrees < 5)).sum(),
                '🔍 Isolé (0-1)': (degrees < 2).sum()
            }
            
            df_cat = pd.DataFrame(
                list(categories.items()),
                columns=['Catégorie', 'Nombre']
            )
            
            fig = px.bar(
                df_cat,
                x='Nombre',
                y='Catégorie',
                orientation='h',
                color='Nombre',
                color_continuous_scale='Oranges',
                text='Nombre'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            total = len(degrees)
            super_connected = categories['🌟 Super-connecté (>10)']
            st.warning(f"**{super_connected}** influenceurs ({super_connected/total*100:.1f}%) dominent le réseau")
    
    st.markdown("---")
    
    # ✅ SECTION 2: INSIGHTS VISUELS AVEC BOXES COLORÉES
    st.markdown('<div class="section-header">🎯 Insights Statistiques</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%); 
                    padding: 1.5rem; border-radius: 12px; border-left: 5px solid #f59e0b;">
            <h4 style="color: #92400e; margin: 0 0 0.5rem 0;">📊 Répartition :</h4>
        </div>
        """, unsafe_allow_html=True)
        
        if not df_posts.empty and 'score' in df_posts.columns:
            scores = df_posts['score']
            q75 = scores.quantile(0.75)
            q25 = scores.quantile(0.25)
            high = (scores > q75).sum()
            low = (scores < q25).sum()
            
            st.markdown(f"""
            - 🔥 **Top 25%** (score > {q75:.0f}) : **{high} posts** ({high/len(scores)*100:.1f}%)
            - 📉 **Bottom 25%** (score < {q25:.0f}) : **{low} posts** ({low/len(scores)*100:.1f}%)
            - 💡 Les posts très populaires (>100 upvotes) sont **rares** mais génèrent beaucoup d'attention
            """)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%); 
                    padding: 1.5rem; border-radius: 12px; border-left: 5px solid #3b82f6;">
            <h4 style="color: #1e40af; margin: 0 0 0.5rem 0;">💬 Engagement :</h4>
        </div>
        """, unsafe_allow_html=True)
        
        if not df_posts.empty and 'num_comments' in df_posts.columns:
            comments = df_posts['num_comments']
            q90 = comments.quantile(0.9)
            very_commented = (comments > q90).sum()
            total_comments = comments.sum()
            top_comments = comments[comments > q90].sum()
            
            st.markdown(f"""
            - 🔥 **Top 10%** (>{q90:.0f} comments) : **{very_commented} posts**
            - 💬 Ces posts génèrent **{top_comments:,}** commentaires ({top_comments/total_comments*100:.1f}% du total)
            - 💡 **{(very_commented/len(comments)*100):.1f}% des posts** → **{(top_comments/total_comments*100):.1f}% des discussions**
            """)
    
    with col3:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%); 
                    padding: 1.5rem; border-radius: 12px; border-left: 5px solid #10b981;">
            <h4 style="color: #065f46; margin: 0 0 0.5rem 0;">🌟 Connectivité :</h4>
        </div>
        """, unsafe_allow_html=True)
        
        if not df_network.empty and 'degree' in df_network.columns:
            degrees = df_network['degree']
            q90 = degrees.quantile(0.9)
            high_degree = (degrees > q90).sum()
            
            st.markdown(f"""
            - 🌟 **Top 10%** (>{q90:.0f} connexions) : **{high_degree} utilisateurs**
            - 💡 Ces **super-connecteurs** sont les influenceurs du réseau
            - 📈 Le reste ({len(degrees)-high_degree} users) a **<{q90:.0f} connexions**
            """)
    
    st.markdown("---")
    
    # ✅ SECTION 3: ANALYSES TEMPORELLES ET GÉOGRAPHIQUES
    st.markdown('<div class="section-header">🌍 Analyses Temporelles & Géographiques</div>', unsafe_allow_html=True)
    
    if not df_posts.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📅 Activité par Jour de la Semaine")
            
            if 'created_date' in df_posts.columns:
                df_posts['day_of_week'] = df_posts['created_date'].dt.day_name()
                day_counts = df_posts['day_of_week'].value_counts()
                
                days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                day_counts = day_counts.reindex(days_order, fill_value=0)
                days_fr = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
                
                fig = px.bar(
                    x=days_fr,
                    y=day_counts.values,
                    labels={'x': 'Jour', 'y': 'Posts'},
                    color=day_counts.values,
                    color_continuous_scale='Purples',
                    text=day_counts.values
                )
                fig.update_traces(textposition='outside')
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
                
                most_active_idx = day_counts.argmax()
                most_active_day = days_fr[most_active_idx]
                st.info(f"🏆 **{most_active_day}** est le jour le plus actif !")
        
        with col2:
            st.markdown("#### 🌍 Pays Africains Mentionnés")
            
            if not df_processed.empty and 'combined_text' in df_processed.columns:
                all_text = ' '.join(df_processed['combined_text'].astype(str)).lower()
                
                countries = {
                    'Maroc': all_text.count('morocco') + all_text.count('maroc'),
                    'Sénégal': all_text.count('senegal') + all_text.count('sénégal'),
                    'Égypte': all_text.count('egypt') + all_text.count('égypte'),
                    'Nigeria': all_text.count('nigeria'),
                    'Cameroun': all_text.count('cameroon') + all_text.count('cameroun'),
                    'Algérie': all_text.count('algeria') + all_text.count('algérie'),
                    'Ghana': all_text.count('ghana'),
                }
                
                countries = {k: v for k, v in countries.items() if v > 0}
                
                if countries:
                    df_countries = pd.DataFrame(
                        list(countries.items()),
                        columns=['Pays', 'Mentions']
                    ).sort_values('Mentions', ascending=True)
                    
                    fig = px.bar(
                        df_countries,
                        x='Mentions',
                        y='Pays',
                        orientation='h',
                        color='Mentions',
                        color_continuous_scale='Oranges',
                        text='Mentions'
                    )
                    fig.update_traces(textposition='outside')
                    fig.update_layout(height=350, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    top = df_countries.iloc[-1]
                    st.success(f"🏆 **{top['Pays']}** domine avec {top['Mentions']} mentions !")
                else:
                    st.warning("⚠️ Aucun pays détecté")
            else:
                st.warning("⚠️ Données non disponibles")
# ===============================
# FOOTER
# ===============================
st.markdown("---")
st.markdown(f"""
<div style="text-align: center; padding: 2rem; background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%); border-radius: 15px; margin-top: 3rem;">
    <p style="font-size: 1.2rem; font-weight: 600; color: #1e293b; margin-bottom: 1rem;">
        ⚽ Reddit CAN 2025 - Big Data Analytics Platform
    </p>
    <p style="color: #64748b; margin-bottom: 1rem;">
        Kafka • Spark Streaming • MongoDB • NetworkX • Machine Learning
    </p>
    <p style="color: #94a3b8; font-size: 0.9rem;">
        📅 Dernière mise à jour: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    </p>
</div>
""", unsafe_allow_html=True)

# ===============================
# AUTO-REFRESH
# ===============================
if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()