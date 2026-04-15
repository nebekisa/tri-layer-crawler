"""
Analytics data models for storing AI processing results.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class SentimentLabel(str, Enum):
    """Sentiment classification labels."""
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    MIXED = "mixed"


class EntityType(str, Enum):
    """Named entity types from spaCy."""
    PERSON = "PERSON"
    ORGANIZATION = "ORG"
    LOCATION = "LOC"
    PRODUCT = "PRODUCT"
    DATE = "DATE"
    MONEY = "MONEY"
    PERCENT = "PERCENT"
    EVENT = "EVENT"
    WORK_OF_ART = "WORK_OF_ART"
    LAW = "LAW"
    LANGUAGE = "LANGUAGE"
    FACILITY = "FACILITY"
    GPE = "GPE"  # Countries, cities, states


@dataclass
class ExtractedEntity:
    """A named entity extracted from text."""
    text: str
    label: EntityType
    start_char: int
    end_char: int
    confidence: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'text': self.text,
            'label': self.label.value if isinstance(self.label, EntityType) else self.label,
            'start_char': self.start_char,
            'end_char': self.end_char,
            'confidence': self.confidence
        }


@dataclass
class SentimentResult:
    """Sentiment analysis result."""
    label: SentimentLabel
    polarity: float  # -1.0 to 1.0
    subjectivity: float  # 0.0 to 1.0
    confidence: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'label': self.label.value,
            'polarity': self.polarity,
            'subjectivity': self.subjectivity,
            'confidence': self.confidence
        }


@dataclass
class TopicClassification:
    """Topic classification result."""
    primary_topic: str
    confidence: float
    alternative_topics: List[tuple] = field(default_factory=list)  # [(topic, score), ...]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'primary_topic': self.primary_topic,
            'confidence': self.confidence,
            'alternative_topics': [{'topic': t, 'score': s} for t, s in self.alternative_topics]
        }


@dataclass
class KeywordResult:
    """Extracted keyword with relevance score."""
    keyword: str
    score: float
    ngram: int  # 1=unigram, 2=bigram, etc.
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'keyword': self.keyword,
            'score': self.score,
            'ngram': self.ngram
        }


@dataclass
class ReadabilityMetrics:
    """Text readability and complexity metrics."""
    flesch_reading_ease: float  # 0-100 (higher = easier)
    flesch_kincaid_grade: float  # US grade level
    gunning_fog: float
    smog_index: float
    automated_readability_index: float
    coleman_liau_index: float
    reading_time_minutes: float
    sentence_count: int
    word_count: int
    complex_word_count: int
    avg_sentence_length: float
    avg_word_length: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'flesch_reading_ease': round(self.flesch_reading_ease, 2),
            'flesch_kincaid_grade': round(self.flesch_kincaid_grade, 2),
            'gunning_fog': round(self.gunning_fog, 2),
            'smog_index': round(self.smog_index, 2),
            'reading_time_minutes': round(self.reading_time_minutes, 2),
            'word_count': self.word_count,
            'sentence_count': self.sentence_count,
        }


@dataclass
class AnalysisResult:
    """Complete analysis result for a crawled item."""
    item_id: int
    url: str
    entities: List[ExtractedEntity]
    sentiment: SentimentResult
    topics: TopicClassification
    keywords: List[KeywordResult]
    readability: ReadabilityMetrics
    content_hash: str  # For duplicate detection
    analyzed_at: datetime
    processing_time_ms: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'item_id': self.item_id,
            'url': self.url,
            'entities': [e.to_dict() for e in self.entities],
            'sentiment': self.sentiment.to_dict(),
            'topics': self.topics.to_dict(),
            'keywords': [k.to_dict() for k in self.keywords[:10]],  # Top 10
            'readability': self.readability.to_dict(),
            'content_hash': self.content_hash,
            'analyzed_at': self.analyzed_at.isoformat() + 'Z',
            'processing_time_ms': self.processing_time_ms
        }