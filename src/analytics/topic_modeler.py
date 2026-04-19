"""
Ultra-lightweight topic modeling - ZERO dependencies.
Uses pure Python keyword extraction and clustering.
"""

import re
import logging
from typing import List, Dict, Optional, Tuple
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)


class TopicModeler:
    """
    Pure Python topic modeling with zero external dependencies.
    """
    
    _model = None
    _topic_labels: Dict[int, str] = {}
    _topic_keywords: Dict[int, List[str]] = {}
    _fitted: bool = False
    
    # Predefined topic patterns for common categories
    TOPIC_PATTERNS = {
        'technology': ['api', 'software', 'data', 'web', 'code', 'programming', 
                       'tech', 'computer', 'digital', 'online', 'app', 'cloud'],
        'business': ['market', 'company', 'business', 'price', 'sale', 'product', 
                     'service', 'customer', 'revenue', 'profit', 'industry'],
        'books': ['book', 'author', 'read', 'novel', 'fiction', 'literature', 
                  'page', 'chapter', 'story', 'writing', 'publisher'],
        'quotes': ['quote', 'say', 'said', 'life', 'love', 'wisdom', 'inspirational',
                   'motivation', 'philosophy', 'think', 'thought'],
        'education': ['learn', 'student', 'teacher', 'school', 'education', 'course',
                      'study', 'university', 'college', 'class', 'lesson'],
        'science': ['research', 'study', 'science', 'experiment', 'analysis', 
                    'data', 'theory', 'method', 'result', 'discovery'],
        'health': ['health', 'medical', 'patient', 'doctor', 'disease', 'treatment',
                   'care', 'hospital', 'medicine', 'wellness'],
        'entertainment': ['movie', 'music', 'film', 'show', 'actor', 'artist',
                          'song', 'video', 'game', 'play', 'entertainment'],
        'shopping': ['price', 'buy', 'shop', 'store', 'product', 'item', 'cart',
                     'checkout', 'order', 'shipping', 'retail'],
        'travel': ['travel', 'hotel', 'flight', 'trip', 'destination', 'tour',
                   'vacation', 'visit', 'tourist', 'guide'],
    }
    
    @classmethod
    def fit(cls, documents: List[str]) -> None:
        """Fit topic model on documents."""
        if not documents:
            logger.warning("No documents provided")
            return
        
        logger.info(f"Fitting lightweight topic model on {len(documents)} documents...")
        
        # Extract topics based on keyword matching
        cls._topic_keywords = {}
        doc_topics = []
        
        for doc in documents:
            doc_lower = doc.lower()
            best_topic = None
            best_score = 0
            
            for topic_name, keywords in cls.TOPIC_PATTERNS.items():
                score = sum(1 for kw in keywords if kw in doc_lower)
                if score > best_score:
                    best_score = score
                    best_topic = topic_name
            
            if best_score > 0:
                doc_topics.append(best_topic)
            else:
                doc_topics.append('general')
        
        # Count topic frequencies
        topic_counts = Counter(doc_topics)
        
        # Assign topic IDs
        cls._topic_labels = {}
        cls._topic_keywords = {}
        
        for idx, (topic_name, count) in enumerate(topic_counts.most_common()):
            cls._topic_labels[idx] = topic_name.title()
            cls._topic_keywords[idx] = cls.TOPIC_PATTERNS.get(topic_name, [])[:5]
        
        # Add unclassified topic
        cls._topic_labels[-1] = "Unclassified"
        cls._topic_keywords[-1] = ["miscellaneous", "general", "other"]
        
        cls._fitted = True
        logger.info(f"? Discovered {len(cls._topic_labels)-1} topics")
    
    @classmethod
    def transform(cls, documents: List[str]) -> List[int]:
        """Assign topics to documents."""
        if not cls._fitted:
            cls.fit(documents)
        
        topics = []
        
        # Reverse mapping from topic name to ID
        name_to_id = {v.lower(): k for k, v in cls._topic_labels.items()}
        
        for doc in documents:
            doc_lower = doc.lower()
            best_topic = 'unclassified'
            best_score = 0
            
            for topic_name, keywords in cls.TOPIC_PATTERNS.items():
                score = sum(1 for kw in keywords if kw in doc_lower)
                if score > best_score:
                    best_score = score
                    best_topic = topic_name
            
            if best_score > 0:
                topic_id = name_to_id.get(best_topic.title(), -1)
            else:
                topic_id = -1
            
            topics.append(topic_id)
        
        return topics
    
    @classmethod
    def get_topic_info(cls) -> List[Dict]:
        """Get topic information."""
        if not cls._fitted:
            return []
        
        info = []
        for topic_id, label in cls._topic_labels.items():
            info.append({
                'topic_id': topic_id,
                'label': label,
                'keywords': cls._topic_keywords.get(topic_id, []),
                'count': 0
            })
        
        return info
    
    @classmethod
    def get_topic_label(cls, topic_id: int) -> str:
        """Get human-readable topic label."""
        return cls._topic_labels.get(topic_id, f"Topic_{topic_id}")


class TopicService:
    """Service for managing topic assignments."""
    
    def __init__(self):
        pass
    
    def fit_on_all_items(self) -> Dict:
        """Fit topic model on all crawled items."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433, database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        cur.execute('SELECT id, content FROM crawled_items WHERE content IS NOT NULL')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        if not rows:
            return {'error': 'No documents found'}
        
        item_ids = [r[0] for r in rows]
        documents = [r[1] for r in rows]
        
        # Fit model
        TopicModeler.fit(documents)
        
        # Get assignments
        topics = TopicModeler.transform(documents)
        
        # Store
        self._store_assignments(item_ids, topics)
        
        return {
            'documents_processed': len(documents),
            'topics_discovered': len(TopicModeler._topic_labels) - 1,
            'topics': TopicModeler.get_topic_info()
        }
    
    def _store_assignments(self, item_ids: List[int], topics: List[int]) -> None:
        """Store topic assignments."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433, database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        # Create table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS item_topics (
                id SERIAL PRIMARY KEY,
                item_id INTEGER NOT NULL REFERENCES crawled_items(id) ON DELETE CASCADE,
                topic_id INTEGER NOT NULL,
                topic_label VARCHAR(200),
                assigned_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(item_id)
            )
        ''')
        
        # Store
        for item_id, topic_id in zip(item_ids, topics):
            label = TopicModeler.get_topic_label(topic_id)
            cur.execute('''
                INSERT INTO item_topics (item_id, topic_id, topic_label)
                VALUES (%s, %s, %s)
                ON CONFLICT (item_id) DO UPDATE
                SET topic_id = EXCLUDED.topic_id,
                    topic_label = EXCLUDED.topic_label,
                    assigned_at = NOW()
            ''', (item_id, topic_id, label))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Stored {len(item_ids)} topic assignments")
    
    def get_topic_distribution(self) -> Dict:
        """Get topic distribution."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433, database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        cur.execute('''
            SELECT topic_id, topic_label, COUNT(*) as count
            FROM item_topics
            GROUP BY topic_id, topic_label
            ORDER BY count DESC
        ''')
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            'topics': [
                {'topic_id': r[0], 'label': r[1], 'count': r[2]}
                for r in rows
            ]
        }
