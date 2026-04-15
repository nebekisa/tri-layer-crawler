"""
Topic clustering using TF-IDF and cosine similarity.
"""

import logging
from typing import List, Dict, Set
from collections import defaultdict
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

from src.intelligence.models import TopicCluster

logger = logging.getLogger(__name__)


class TopicClusterer:
    """
    Cluster documents into topics using TF-IDF.
    
    Features:
        - Automatic topic discovery
        - Keyword extraction per cluster
        - Coherence scoring
        - Representative document selection
    """
    
    def __init__(self, n_clusters: int = 5, random_state: int = 42):
        """
        Initialize clusterer.
        
        Args:
            n_clusters: Number of topic clusters
            random_state: Random seed for reproducibility
        """
        self.n_clusters = n_clusters
        self.random_state = random_state
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)
        )
    
    def cluster(
        self,
        documents: List[Dict]
    ) -> List[TopicCluster]:
        """
        Cluster documents into topics.
        
        Args:
            documents: List of {'url': str, 'content': str, 'keywords': List[str]}
            
        Returns:
            List of topic clusters
        """
        if len(documents) < self.n_clusters:
            logger.warning(f"Not enough documents for clustering ({len(documents)} < {self.n_clusters})")
            return []
        
        # Extract text content
        texts = [doc['content'] for doc in documents]
        
        # Vectorize
        tfidf_matrix = self.vectorizer.fit_transform(texts)
        
        # Cluster
        kmeans = KMeans(
            n_clusters=min(self.n_clusters, len(documents)),
            random_state=self.random_state,
            n_init=10
        )
        clusters = kmeans.fit_predict(tfidf_matrix)
        
        # Get feature names
        feature_names = self.vectorizer.get_feature_names_out()
        
        # Build clusters
        topic_clusters = []
        
        for cluster_id in range(kmeans.n_clusters):
            # Get documents in this cluster
            cluster_docs = [
                documents[i] for i in range(len(documents))
                if clusters[i] == cluster_id
            ]
            
            # Get top terms for this cluster
            centroid = kmeans.cluster_centers_[cluster_id]
            top_indices = centroid.argsort()[-10:][::-1]
            top_terms = [feature_names[i] for i in top_indices]
            
            # Find primary topic (most frequent keyword)
            all_keywords = []
            for doc in cluster_docs:
                all_keywords.extend(doc.get('keywords', []))
            
            primary_topic = self._get_primary_topic(all_keywords, top_terms)
            
            # Select representative documents (closest to centroid)
            cluster_matrix = tfidf_matrix[clusters == cluster_id]
            centroid_vec = centroid.reshape(1, -1)
            similarities = cosine_similarity(cluster_matrix, centroid_vec).flatten()
            top_doc_indices = similarities.argsort()[-3:][::-1]
            
            representative_docs = [
                cluster_docs[i]['url']
                for i in top_doc_indices
                if i < len(cluster_docs)
            ]
            
            # Calculate coherence (simplified)
            coherence = self._calculate_coherence(top_terms)
            
            cluster = TopicCluster(
                cluster_id=int(cluster_id),
                primary_topic=primary_topic,
                keywords=top_terms[:7],
                document_count=len(cluster_docs),
                representative_docs=representative_docs,
                coherence_score=coherence
            )
            
            topic_clusters.append(cluster)
        
        # Sort by document count
        topic_clusters.sort(key=lambda x: x.document_count, reverse=True)
        
        logger.info(f"Clustered {len(documents)} docs into {len(topic_clusters)} topics")
        
        return topic_clusters
    
    def _get_primary_topic(self, keywords: List[str], top_terms: List[str]) -> str:
        """Determine primary topic name."""
        from collections import Counter
        
        if keywords:
            # Use most common keyword
            counter = Counter(keywords)
            return counter.most_common(1)[0][0]
        elif top_terms:
            # Use top TF-IDF term
            return top_terms[0]
        else:
            return "Unknown Topic"
    
    def _calculate_coherence(self, terms: List[str]) -> float:
        """
        Calculate topic coherence score.
        Simplified version - in production use gensim or palmetto.
        """
        if len(terms) < 2:
            return 0.5
        
        # Simple heuristic: longer terms tend to be more specific
        avg_length = sum(len(t) for t in terms) / len(terms)
        coherence = min(1.0, avg_length / 15)
        
        return round(coherence, 3)