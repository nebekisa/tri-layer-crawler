"""
SQLAlchemy ORM models for crawled data and analytics.
Single source of truth - no duplicates, clean relationships.
"""

from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Integer, DateTime, Text,
    BigInteger, Float, Index, ForeignKey
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

Base = declarative_base()


class CrawledItem(Base):
    """Primary model for storing crawled web pages."""
    
    __tablename__ = 'crawled_items'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    url = Column(String(2048), nullable=False, unique=True, index=True)
    title = Column(String(512), nullable=False)
    content = Column(Text, nullable=False)
    meta_description = Column(String(1024), nullable=True)
    domain = Column(String(255), nullable=False, index=True)
    status_code = Column(Integer, nullable=False)
    content_length = Column(Integer, nullable=False)
    crawled_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Relationship to analysis (one-to-one)
    analysis = relationship("AnalysisResultDB", back_populates="item", uselist=False, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<CrawledItem(id={self.id}, url='{self.url[:50]}...')>"
    
    def to_dict(self):
        """Convert to dictionary for API responses."""
        return {
            'id': self.id,
            'url': self.url,
            'title': self.title,
            'content': self.content,
            'meta_description': self.meta_description or '',
            'domain': self.domain,
            'status_code': self.status_code,
            'content_length': self.content_length,
            'crawled_at': self.crawled_at.isoformat() + 'Z' if self.crawled_at else None,
        }


class AnalysisResultDB(Base):
    """Store AI analysis results for crawled items."""
    
    __tablename__ = 'analysis_results'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    item_id = Column(BigInteger, ForeignKey('crawled_items.id', ondelete='CASCADE'), nullable=False, unique=True, index=True)
    
    # Sentiment
    sentiment_label = Column(String(20), nullable=False)
    sentiment_polarity = Column(Float, nullable=False)
    sentiment_subjectivity = Column(Float, nullable=False)
    sentiment_confidence = Column(Float, nullable=False)
    
    # Topics
    primary_topic = Column(String(100), nullable=True)
    topic_confidence = Column(Float, nullable=True)
    
    # Readability
    flesch_reading_ease = Column(Float, nullable=True)
    flesch_kincaid_grade = Column(Float, nullable=True)
    reading_time_minutes = Column(Float, nullable=True)
    word_count = Column(Integer, nullable=True)
    sentence_count = Column(Integer, nullable=True)
    
    # Metadata
    content_hash = Column(String(64), nullable=False, index=True)
    analyzed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    processing_time_ms = Column(Float, nullable=True)
    
    # Relationships
    item = relationship("CrawledItem", back_populates="analysis")
    entities = relationship("ExtractedEntityDB", back_populates="analysis", cascade="all, delete-orphan")
    keywords = relationship("ExtractedKeywordDB", back_populates="analysis", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<AnalysisResultDB(item_id={self.item_id}, sentiment={self.sentiment_label})>"


class ExtractedEntityDB(Base):
    """Store named entities extracted from content."""
    
    __tablename__ = 'extracted_entities'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id = Column(BigInteger, ForeignKey('analysis_results.id', ondelete='CASCADE'), nullable=False, index=True)
    entity_text = Column(String(255), nullable=False, index=True)
    entity_type = Column(String(50), nullable=False, index=True)
    confidence = Column(Float, default=1.0)
    occurrence_count = Column(Integer, default=1)
    
    # Relationship
    analysis = relationship("AnalysisResultDB", back_populates="entities")
    
    __table_args__ = (
        Index('idx_entity_text_type', 'entity_text', 'entity_type'),
    )
    
    def __repr__(self):
        return f"<ExtractedEntityDB(text='{self.entity_text}', type={self.entity_type})>"


class ExtractedKeywordDB(Base):
    """Store keywords extracted from content."""
    
    __tablename__ = 'extracted_keywords'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    analysis_id = Column(BigInteger, ForeignKey('analysis_results.id', ondelete='CASCADE'), nullable=False, index=True)
    keyword = Column(String(255), nullable=False, index=True)
    score = Column(Float, nullable=False)
    ngram = Column(Integer, nullable=False)
    
    # Relationship
    analysis = relationship("AnalysisResultDB", back_populates="keywords")
    
    __table_args__ = (
        Index('idx_keyword_score', 'keyword', 'score'),
    )
    
    def __repr__(self):
        return f"<ExtractedKeywordDB(keyword='{self.keyword}', score={self.score:.3f})>"