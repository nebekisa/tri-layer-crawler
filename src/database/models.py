"""
SQLAlchemy ORM models for crawled data.
Compatible with both SQLite and PostgreSQL.
"""

from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class CrawledItem(Base):
    """Model for storing crawled web pages."""
    
    __tablename__ = 'crawled_items'
    
    # Use BigInteger for auto-increment (better for PostgreSQL)
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    url = Column(String(2048), nullable=False, unique=True, index=True)
    title = Column(String(512), nullable=False)
    content = Column(Text, nullable=False)
    meta_description = Column(String(1024), nullable=True)
    domain = Column(String(255), nullable=False, index=True)
    status_code = Column(Integer, nullable=False)
    content_length = Column(Integer, nullable=False)
    
    # Use server_default for better cross-database compatibility
    crawled_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<CrawledItem(url='{self.url}', title='{self.title[:30]}...')>"
    
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