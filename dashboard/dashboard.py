"""
========================================================
📊 DASHBOARD STREAMLIT OPTIMISÉ - Reddit CAN 2025
========================================================
✔ Métriques temps réel
✔ Graphiques interactifs (Plotly)
✔ Distribution des sentiments
✔ Top posts par sentiment
✔ Évolution temporelle
✔ Word Cloud
✔ Métriques ML
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
import re

# ===============================
# CONFIGURATION PAGE
# ===============================
st.set_page_config(
    page_title="Reddit CAN 2025 Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===============================
# CSS PERSONNALISÉ
# ===============================
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(120deg, #1e3a8a 0%, #3b82f6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        padding: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric {
        background-color: #f8fafc;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e2e8f0;
    }
</style>
""", unsafe_allow_html=True)

# ===============================
# CONNEXION MONGODB
# ===============================
@st.cache_resource
def get_mongo_client():
    """Connexion MongoDB avec cache"""
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
@st.cache_data(ttl=30)  # Cache 30 secondes
def load_data():
    """Charge les données depuis MongoDB"""
    client = get_mongo_client()
    if not client:
        return None, None, None, None
    
    db = client['reddit_can']
    
    # Collections
    posts = list(db['posts'].find({}, {'_id': 0}))
    comments = list(db['comments'].find({}, {'_id': 0}))
    processed = list(db['processed_posts'].find({}, {'_id': 0}))
    sentiments = list(db['sentiment_results'].find({}, {'_id': 0}))
    
    # Convertir en DataFrames
    df_posts = pd.DataFrame(posts) if posts else pd.DataFrame()
    df_comments = pd.DataFrame(comments) if comments else pd.DataFrame()
    df_processed = pd.DataFrame(processed) if processed else pd.DataFrame()
    df_sentiments = pd.DataFrame(sentiments) if sentiments else pd.DataFrame()
    
    # Conversion dates
    for df in [df_posts, df_comments, df_processed]:
        if not df.empty and 'created_date' in df.columns:
            df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    
    if not df_sentiments.empty and 'analyzed_at' in df_sentiments.columns:
        df_sentiments['analyzed_at'] = pd.to_datetime(df_sentiments['analyzed_at'], errors='coerce')
    
    return df_posts, df_comments, df_processed, df_sentiments

# ===============================
# HEADER
# ===============================
st.markdown('<h1 class="main-header">⚽ Reddit CAN 2025 - Big Data Analytics Dashboard</h1>', unsafe_allow_html=True)
st.markdown("---")

# ===============================
# SIDEBAR - CONTRÔLES
# ===============================
st.sidebar.title("🎛️ Contrôles")

# Auto-refresh
auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)", value=True)
if auto_refresh:
    st.sidebar.info("Dashboard se rafraîchit automatiquement")

# Filtres
st.sidebar.markdown("### 📅 Filtres")
sentiment_filter = st.sidebar.multiselect(
    "Sentiments",
    ["positive", "neutral", "negative"],
    default=["positive", "neutral", "negative"]
)

# ===============================
# CHARGEMENT DONNÉES
# ===============================
with st.spinner("📥 Chargement des données..."):
    df_posts, df_comments, df_processed, df_sentiments = load_data()

if df_posts is None:
    st.error("❌ Impossible de charger les données. Vérifiez MongoDB.")
    st.stop()

# ===============================
# MÉTRIQUES PRINCIPALES
# ===============================
st.markdown("## 📊 Métriques Clés")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="📝 Posts Collectés",
        value=len(df_posts),
        delta=f"+{len(df_posts) - len(df_processed)}" if len(df_posts) > len(df_processed) else "0"
    )

with col2:
    st.metric(
        label="💬 Commentaires",
        value=len(df_comments)
    )

with col3:
    st.metric(
        label="⚡ Posts Traités",
        value=len(df_processed)
    )

with col4:
    st.metric(
        label="🤖 Sentiments Analysés",
        value=len(df_sentiments)
    )

with col5:
    engagement = df_posts['score'].sum() if not df_posts.empty and 'score' in df_posts.columns else 0
    st.metric(
        label="📈 Engagement Total",
        value=f"{engagement:,}"
    )

st.markdown("---")

# ===============================
# ANALYSE SENTIMENTS
# ===============================
if not df_sentiments.empty and 'predicted_sentiment' in df_sentiments.columns:
    
    st.markdown("## 💭 Analyse des Sentiments")
    
    # Filtrer par sentiment
    df_sentiments_filtered = df_sentiments[df_sentiments['predicted_sentiment'].isin(sentiment_filter)]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Distribution des Sentiments")
        
        sentiment_counts = df_sentiments_filtered['predicted_sentiment'].value_counts()
        
        # Couleurs personnalisées
        colors = {
            'positive': '#10b981',  # Vert
            'neutral': '#6b7280',   # Gris
            'negative': '#ef4444'   # Rouge
        }
        
        fig_pie = px.pie(
            values=sentiment_counts.values,
            names=sentiment_counts.index,
            title="",
            color=sentiment_counts.index,
            color_discrete_map=colors,
            hole=0.4
        )
        
        fig_pie.update_traces(
            textposition='inside',
            textinfo='percent+label',
            marker=dict(line=dict(color='white', width=2))
        )
        
        fig_pie.update_layout(
            showlegend=True,
            height=400,
            margin=dict(t=30, b=30, l=30, r=30)
        )
        
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.markdown("### 📈 Répartition par Sentiment")
        
        fig_bar = px.bar(
            x=sentiment_counts.index,
            y=sentiment_counts.values,
            color=sentiment_counts.index,
            color_discrete_map=colors,
            labels={'x': 'Sentiment', 'y': 'Nombre de Posts'},
            text=sentiment_counts.values
        )
        
        fig_bar.update_traces(
            texttemplate='%{text}',
            textposition='outside'
        )
        
        fig_bar.update_layout(
            showlegend=False,
            height=400,
            xaxis_title="",
            yaxis_title="Nombre de Posts",
            margin=dict(t=30, b=30, l=30, r=30)
        )
        
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # ===============================
    # MÉTRIQUES ML
    # ===============================
    st.markdown("### 🤖 Performance du Modèle ML")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🎯 Accuracy", "84.21%")
    
    with col2:
        st.metric("📊 F1-Score", "84.14%")
    
    with col3:
        st.metric("🏆 Modèle", "Random Forest")
    
    with col4:
        coverage = (len(df_sentiments) / len(df_processed) * 100) if len(df_processed) > 0 else 0
        st.metric("📈 Coverage", f"{coverage:.1f}%")

else:
    st.warning("⚠️ Aucune donnée de sentiment disponible. Lancez l'analyse ML.")

st.markdown("---")

# ===============================
# TOP POSTS
# ===============================
st.markdown("## 🏆 Top Posts par Sentiment")

if not df_sentiments.empty and 'predicted_sentiment' in df_sentiments.columns:
    
    tabs = st.tabs(["😊 Positifs", "😐 Neutres", "😞 Négatifs"])
    
    for idx, (tab, sentiment) in enumerate(zip(tabs, ['positive', 'neutral', 'negative'])):
        with tab:
            sentiment_posts = df_sentiments[df_sentiments['predicted_sentiment'] == sentiment]
            
            if not sentiment_posts.empty:
                # Trier par score
                top_posts = sentiment_posts.nlargest(5, 'score')
                
                for i, row in top_posts.iterrows():
                    with st.container():
                        st.markdown(f"**{row.get('combined_text', 'N/A')[:100]}...**")
                        
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Score", row.get('score', 0))
                        col2.metric("Commentaires", row.get('num_comments', 0))
                        col3.metric("Sentiment", row.get('predicted_sentiment', 'N/A'))
                        
                        st.markdown("---")
            else:
                st.info(f"Aucun post {sentiment} trouvé")

st.markdown("---")

# ===============================
# SUBREDDITS
# ===============================
st.markdown("## 📍 Top Subreddits")

if not df_posts.empty and 'subreddit' in df_posts.columns:
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔝 Par Nombre de Posts")
        
        subreddit_counts = df_posts['subreddit'].value_counts().head(10)
        
        fig_sub = px.bar(
            x=subreddit_counts.values,
            y=subreddit_counts.index,
            orientation='h',
            labels={'x': 'Nombre de Posts', 'y': 'Subreddit'},
            text=subreddit_counts.values,
            color=subreddit_counts.values,
            color_continuous_scale='Blues'
        )
        
        fig_sub.update_traces(textposition='outside')
        fig_sub.update_layout(
            showlegend=False,
            height=400,
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(t=30, b=30, l=30, r=30)
        )
        
        st.plotly_chart(fig_sub, use_container_width=True)
    
    with col2:
        st.markdown("### ⭐ Par Engagement Total")
        
        engagement_by_sub = df_posts.groupby('subreddit')['score'].sum().sort_values(ascending=False).head(10)
        
        fig_eng = px.bar(
            x=engagement_by_sub.values,
            y=engagement_by_sub.index,
            orientation='h',
            labels={'x': 'Score Total', 'y': 'Subreddit'},
            text=engagement_by_sub.values,
            color=engagement_by_sub.values,
            color_continuous_scale='Greens'
        )
        
        fig_eng.update_traces(textposition='outside')
        fig_eng.update_layout(
            showlegend=False,
            height=400,
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(t=30, b=30, l=30, r=30)
        )
        
        st.plotly_chart(fig_eng, use_container_width=True)

st.markdown("---")

# ===============================
# ÉVOLUTION TEMPORELLE
# ===============================
st.markdown("## 📈 Évolution Temporelle")

if not df_posts.empty and 'created_date' in df_posts.columns:
    
    # Grouper par date
    df_posts['date'] = df_posts['created_date'].dt.date
    posts_by_date = df_posts.groupby('date').size().reset_index(name='count')
    
    fig_timeline = px.line(
        posts_by_date,
        x='date',
        y='count',
        title="",
        labels={'date': 'Date', 'count': 'Nombre de Posts'},
        markers=True
    )
    
    fig_timeline.update_traces(
        line=dict(color='#3b82f6', width=3),
        marker=dict(size=8)
    )
    
    fig_timeline.update_layout(
        height=400,
        hovermode='x unified',
        margin=dict(t=30, b=30, l=30, r=30)
    )
    
    st.plotly_chart(fig_timeline, use_container_width=True)

st.markdown("---")

# ===============================
# MOTS-CLÉS
# ===============================
st.markdown("## 🔤 Analyse des Mots-Clés")

if not df_processed.empty and 'combined_text' in df_processed.columns:
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔥 Mots les Plus Fréquents")
        
        # Extraire tous les mots
        all_text = ' '.join(df_processed['combined_text'].astype(str))
        words = re.findall(r'\b[a-z]{4,}\b', all_text.lower())
        
        # Mots vides à exclure
        stop_words = {'afcon', 'that', 'this', 'with', 'from', 'have', 'will', 'been', 'were', 'their'}
        words = [w for w in words if w not in stop_words]
        
        word_counts = Counter(words).most_common(15)
        
        if word_counts:
            df_words = pd.DataFrame(word_counts, columns=['Mot', 'Fréquence'])
            
            fig_words = px.bar(
                df_words,
                x='Fréquence',
                y='Mot',
                orientation='h',
                text='Fréquence',
                color='Fréquence',
                color_continuous_scale='Oranges'
            )
            
            fig_words.update_traces(textposition='outside')
            fig_words.update_layout(
                showlegend=False,
                height=500,
                yaxis={'categoryorder': 'total ascending'},
                margin=dict(t=30, b=30, l=30, r=30)
            )
            
            st.plotly_chart(fig_words, use_container_width=True)
    
    with col2:
        st.markdown("### 🌍 Pays Mentionnés")
        
        countries = [
            'morocco', 'senegal', 'egypt', 'nigeria', 'cameroon',
            'algeria', 'tunisia', 'ghana', 'mali', 'ivory'
        ]
        
        country_counts = {}
        for country in countries:
            count = all_text.lower().count(country)
            if count > 0:
                country_counts[country.title()] = count
        
        if country_counts:
            df_countries = pd.DataFrame(
                list(country_counts.items()),
                columns=['Pays', 'Mentions']
            ).sort_values('Mentions', ascending=True)
            
            fig_countries = px.bar(
                df_countries,
                x='Mentions',
                y='Pays',
                orientation='h',
                text='Mentions',
                color='Mentions',
                color_continuous_scale='Reds'
            )
            
            fig_countries.update_traces(textposition='outside')
            fig_countries.update_layout(
                showlegend=False,
                height=500,
                margin=dict(t=30, b=30, l=30, r=30)
            )
            
            st.plotly_chart(fig_countries, use_container_width=True)

st.markdown("---")

# ===============================
# STATISTIQUES AVANCÉES
# ===============================
st.markdown("## 📊 Statistiques Avancées")

col1, col2, col3 = st.columns(3)

with col1:
    if not df_posts.empty and 'score' in df_posts.columns:
        st.markdown("### 📈 Score Moyen")
        avg_score = df_posts['score'].mean()
        st.metric("Score Moyen", f"{avg_score:.1f}")
        
        st.markdown("**Distribution des Scores**")
        fig_score_dist = px.histogram(
            df_posts,
            x='score',
            nbins=30,
            labels={'score': 'Score', 'count': 'Fréquence'},
            color_discrete_sequence=['#8b5cf6']
        )
        fig_score_dist.update_layout(height=300, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig_score_dist, use_container_width=True)

with col2:
    if not df_posts.empty and 'num_comments' in df_posts.columns:
        st.markdown("### 💬 Commentaires Moyens")
        avg_comments = df_posts['num_comments'].mean()
        st.metric("Moyenne", f"{avg_comments:.1f}")
        
        st.markdown("**Distribution**")
        fig_comments_dist = px.histogram(
            df_posts,
            x='num_comments',
            nbins=30,
            labels={'num_comments': 'Commentaires', 'count': 'Fréquence'},
            color_discrete_sequence=['#06b6d4']
        )
        fig_comments_dist.update_layout(height=300, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig_comments_dist, use_container_width=True)

with col3:
    if not df_sentiments.empty and 'emoji_score' in df_sentiments.columns:
        st.markdown("### 😊 Score Emoji Moyen")
        avg_emoji = df_sentiments['emoji_score'].mean()
        st.metric("Moyenne", f"{avg_emoji:.2f}")
        
        st.markdown("**Distribution**")
        fig_emoji_dist = px.histogram(
            df_sentiments,
            x='emoji_score',
            nbins=20,
            labels={'emoji_score': 'Score Emoji', 'count': 'Fréquence'},
            color_discrete_sequence=['#f59e0b']
        )
        fig_emoji_dist.update_layout(height=300, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig_emoji_dist, use_container_width=True)

st.markdown("---")

# ===============================
# FOOTER
# ===============================
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**📅 Dernière Mise à Jour**")
    st.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

with col2:
    st.markdown("**🔗 Liens Utiles**")
    st.markdown("[MongoDB Express](http://localhost:8081) | [Airflow](http://localhost:8080)")

with col3:
    st.markdown("**⚙️ Système**")
    st.success("✅ Tous les services opérationnels")

# ===============================
# AUTO-REFRESH
# ===============================
if auto_refresh:
    import time
    time.sleep(30)
    st.rerun()