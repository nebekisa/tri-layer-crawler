"""
Readability and text complexity metrics using textstat.
"""

import logging
import textstat
from typing import Dict, Any

from src.analytics.models import ReadabilityMetrics

logger = logging.getLogger(__name__)


class ReadabilityAnalyzer:
    """
    Calculate readability and complexity metrics for text.
    
    Metrics:
        - Flesch Reading Ease (0-100, higher = easier)
        - Flesch-Kincaid Grade Level (US school grade)
        - Gunning Fog Index
        - SMOG Index
        - Automated Readability Index
        - Coleman-Liau Index
        - Reading time (minutes)
    """
    
    def __init__(self):
        """Initialize analyzer."""
        textstat.set_lang('en')
    
    def analyze(self, text: str) -> ReadabilityMetrics:
        """
        Calculate all readability metrics for text.
        
        Args:
            text: Input text to analyze
            
        Returns:
            ReadabilityMetrics object with all scores
        """
        if not text or not text.strip():
            return self._empty_metrics()
        
        # Truncate for performance (textstat handles any length)
        max_chars = 100000
        if len(text) > max_chars:
            text = text[:max_chars]
        
        try:
            # Basic counts
            word_count = textstat.lexicon_count(text, removepunct=True)
            sentence_count = textstat.sentence_count(text)
            
            # Readability scores
            flesch_ease = textstat.flesch_reading_ease(text)
            flesch_grade = textstat.flesch_kincaid_grade(text)
            gunning_fog = textstat.gunning_fog(text)
            smog_index = textstat.smog_index(text)
            automated_ri = textstat.automated_readability_index(text)
            coleman_liau = textstat.coleman_liau_index(text)
            
            # Reading time (average 238 words per minute)
            reading_time = word_count / 238.0
            
            # Complexity metrics
            complex_words = textstat.difficult_words(text)
            avg_sentence_length = word_count / max(sentence_count, 1)
            
            # Average word length
            words = text.split()
            avg_word_length = sum(len(w) for w in words) / max(len(words), 1)
            
            return ReadabilityMetrics(
                flesch_reading_ease=round(flesch_ease, 2),
                flesch_kincaid_grade=round(flesch_grade, 2),
                gunning_fog=round(gunning_fog, 2),
                smog_index=round(smog_index, 2),
                automated_readability_index=round(automated_ri, 2),
                coleman_liau_index=round(coleman_liau, 2),
                reading_time_minutes=round(reading_time, 2),
                sentence_count=sentence_count,
                word_count=word_count,
                complex_word_count=complex_words,
                avg_sentence_length=round(avg_sentence_length, 2),
                avg_word_length=round(avg_word_length, 2)
            )
            
        except Exception as e:
            logger.error(f"Readability analysis failed: {e}")
            return self._empty_metrics()
    
    def _empty_metrics(self) -> ReadabilityMetrics:
        """Return empty metrics for invalid text."""
        return ReadabilityMetrics(
            flesch_reading_ease=0.0,
            flesch_kincaid_grade=0.0,
            gunning_fog=0.0,
            smog_index=0.0,
            automated_readability_index=0.0,
            coleman_liau_index=0.0,
            reading_time_minutes=0.0,
            sentence_count=0,
            word_count=0,
            complex_word_count=0,
            avg_sentence_length=0.0,
            avg_word_length=0.0
        )
    
    def get_reading_level_summary(self, metrics: ReadabilityMetrics) -> str:
        """
        Get human-readable reading level description.
        
        Args:
            metrics: ReadabilityMetrics object
            
        Returns:
            Description like "8th-9th Grade" or "College Level"
        """
        grade = metrics.flesch_kincaid_grade
        
        if grade < 1:
            return "Kindergarten"
        elif grade < 6:
            return f"Grade {int(grade)} (Elementary)"
        elif grade < 9:
            return f"Grade {int(grade)} (Middle School)"
        elif grade < 12:
            return f"Grade {int(grade)} (High School)"
        elif grade < 16:
            return "College Level"
        else:
            return "Graduate Level"
    
    def get_difficulty_label(self, metrics: ReadabilityMetrics) -> str:
        """
        Get difficulty label based on Flesch Reading Ease.
        
        Args:
            metrics: ReadabilityMetrics object
            
        Returns:
            'Very Easy', 'Easy', 'Fairly Easy', 'Standard',
            'Fairly Difficult', 'Difficult', 'Very Difficult'
        """
        score = metrics.flesch_reading_ease
        
        if score >= 90:
            return "Very Easy"
        elif score >= 80:
            return "Easy"
        elif score >= 70:
            return "Fairly Easy"
        elif score >= 60:
            return "Standard"
        elif score >= 50:
            return "Fairly Difficult"
        elif score >= 30:
            return "Difficult"
        else:
            return "Very Difficult"