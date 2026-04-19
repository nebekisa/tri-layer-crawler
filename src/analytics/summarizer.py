"""
Lightweight summarization using extractive methods.
No downloads, no GPU, works immediately.
"""

import re
import logging
from collections import Counter
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


class LightweightSummarizer:
    """
    Extractive text summarization without ML models.
    
    Uses sentence scoring based on:
    - Word frequency
    - Sentence position
    - Keyword presence
    """
    
    @staticmethod
    def summarize(text: str, num_sentences: int = 3, min_chars: int = 100) -> Optional[str]:
        """
        Extract top sentences as summary.
        
        Args:
            text: Input text to summarize
            num_sentences: Number of sentences in summary
            min_chars: Minimum text length to summarize
            
        Returns:
            Summary string or None if text too short
        """
        if not text or len(text) < min_chars:
            return None
        
        # Split into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 20]
        
        if len(sentences) <= num_sentences:
            return ' '.join(sentences)
        
        # Word frequency scoring
        words = re.findall(r'\w+', text.lower())
        word_freq = Counter(words)
        
        # Remove common stopwords from scoring
        stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
                     'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were'}
        for sw in stopwords:
            word_freq.pop(sw, None)
        
        # Score each sentence
        scored_sentences = []
        for i, sent in enumerate(sentences):
            sent_words = re.findall(r'\w+', sent.lower())
            
            # Word frequency score
            freq_score = sum(word_freq.get(w, 0) for w in sent_words)
            freq_score = freq_score / max(len(sent_words), 1)
            
            # Position bonus (first and last sentences are often important)
            position_bonus = 1.0
            if i == 0:
                position_bonus = 1.5
            elif i == len(sentences) - 1:
                position_bonus = 1.3
            
            # Length penalty (avoid very short sentences)
            length_penalty = min(1.0, len(sent) / 100)
            
            total_score = freq_score * position_bonus * length_penalty
            scored_sentences.append((sent, total_score, i))
        
        # Sort by score and select top sentences
        scored_sentences.sort(key=lambda x: x[1], reverse=True)
        selected = scored_sentences[:num_sentences]
        
        # Restore original order
        selected.sort(key=lambda x: x[2])
        
        return ' '.join([s[0] for s in selected])


# Replace the heavy Summarizer with lightweight version
class Summarizer:
    """Summarizer using lightweight extractive methods."""
    
    MODEL_NAME = "extractive-lightweight"
    _cache: Dict[str, str] = {}
    
    @classmethod
    def summarize(cls, text: str, max_length: int = 150, min_length: int = 40) -> Optional[str]:
        """Generate summary using lightweight method."""
        import hashlib
        
        if not text or len(text.strip()) < 100:
            return None
        
        # Check cache
        text_hash = hashlib.md5(text.encode()).hexdigest()
        if text_hash in cls._cache:
            return cls._cache[text_hash]
        
        # Generate summary
        num_sentences = 3 if len(text) < 1000 else 4
        summary = LightweightSummarizer.summarize(text, num_sentences=num_sentences)
        
        if summary:
            cls._cache[text_hash] = summary
            logger.info(f"Generated summary: {len(summary)} chars")
        
        return summary
    
    @classmethod
    def get_model_info(cls):
        return {
            "model_name": cls.MODEL_NAME,
            "loaded": True,
            "cache_size": len(cls._cache)
        }


# Keep SummaryService unchanged
class SummaryService:
    """Service for managing content summaries."""
    
    def __init__(self):
        pass
    
    def generate_and_store(self, item_id: int, content: str) -> Optional[Dict]:
        """Generate summary and store in database."""
        import psycopg2
        from datetime import datetime
        
        summary_text = Summarizer.summarize(content)
        
        if not summary_text:
            logger.warning(f"No summary generated for item {item_id}")
            return None
        
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5433,
                database='tri_layer_crawler',
                user='crawler_user',
                password='CrawlerPass2024!'
            )
            cur = conn.cursor()
            
            # Create table if not exists
            cur.execute('''
                CREATE TABLE IF NOT EXISTS content_summaries (
                    id SERIAL PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    summary TEXT NOT NULL,
                    model VARCHAR(100) NOT NULL,
                    generated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(item_id)
                )
            ''')
            
            # Insert or update
            cur.execute('''
                INSERT INTO content_summaries (item_id, summary, model)
                VALUES (%s, %s, %s)
                ON CONFLICT (item_id) DO UPDATE
                SET summary = EXCLUDED.summary, 
                    model = EXCLUDED.model,
                    generated_at = NOW()
                RETURNING id
            ''', (item_id, summary_text, Summarizer.MODEL_NAME))
            
            summary_id = cur.fetchone()[0]
            conn.commit()
            
            cur.close()
            conn.close()
            
            logger.info(f"Stored summary for item {item_id}")
            
            return {
                "id": summary_id,
                "item_id": item_id,
                "summary": summary_text,
                "model": Summarizer.MODEL_NAME,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to store summary: {e}")
            return None
    
    def get_summary(self, item_id: int) -> Optional[Dict]:
        """Retrieve summary for an item."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5433,
                database='tri_layer_crawler',
                user='crawler_user',
                password='CrawlerPass2024!'
            )
            cur = conn.cursor()
            
            cur.execute('''
                SELECT id, summary, model, generated_at
                FROM content_summaries
                WHERE item_id = %s
            ''', (item_id,))
            
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                return {
                    "id": row[0],
                    "item_id": item_id,
                    "summary": row[1],
                    "model": row[2],
                    "generated_at": row[3].isoformat() if row[3] else None
                }
            return None
            
        except Exception as e:
            logger.error(f"Failed to get summary: {e}")
            return None
