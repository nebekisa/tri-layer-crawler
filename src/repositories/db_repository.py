"""
Database repository for crawled items.
"""

from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import desc

from src.database.models import CrawledItem
from src.database.manager import get_db_session


class DatabaseRepository:
    """
    Repository for database operations on crawled items.
    """
    
    def __init__(self):
        self.session: Session = get_db_session()
    
    def save(self, item_data: Dict) -> CrawledItem:
        """
        Save a crawled item to the database.
        
        If the URL already exists, update the existing record.
        """
        # Check if URL already exists
        existing = self.session.query(CrawledItem).filter(
            CrawledItem.url == item_data['url']
        ).first()
        
        if existing:
            # Update existing
            existing.title = item_data['title']
            existing.content = item_data['content']
            existing.meta_description = item_data.get('meta_description', '')
            existing.status_code = item_data['status_code']
            existing.content_length = len(item_data['content'])
            item = existing
        else:
            # Create new
            item = CrawledItem(
                url=item_data['url'],
                title=item_data['title'],
                content=item_data['content'],
                meta_description=item_data.get('meta_description', ''),
                domain=item_data.get('domain', ''),
                status_code=item_data['status_code'],
                content_length=len(item_data['content']),
            )
            self.session.add(item)
        
        self.session.commit()
        self.session.refresh(item)
        return item
    
    def save_batch(self, items_data: List[Dict]) -> int:
        """
        Save multiple items in a single transaction.
        
        Returns:
            Number of items saved.
        """
        saved_count = 0
        for item_data in items_data:
            try:
                self.save(item_data)
                saved_count += 1
            except Exception as e:
                self.session.rollback()
                print(f"Error saving {item_data.get('url')}: {e}")
        
        return saved_count
    
    def get_all(self, limit: int = 100, offset: int = 0) -> List[CrawledItem]:
        """Get all items with pagination."""
        return self.session.query(CrawledItem)\
            .order_by(desc(CrawledItem.crawled_at))\
            .offset(offset)\
            .limit(limit)\
            .all()
    
    def get_by_url(self, url: str) -> Optional[CrawledItem]:
        """Get a specific item by URL."""
        return self.session.query(CrawledItem).filter(
            CrawledItem.url == url
        ).first()
    
    def get_by_domain(self, domain: str) -> List[CrawledItem]:
        """Get all items from a specific domain."""
        return self.session.query(CrawledItem).filter(
            CrawledItem.domain == domain
        ).all()
    
    def count(self) -> int:
        """Get total count of items."""
        return self.session.query(CrawledItem).count()
    
    def delete_by_url(self, url: str) -> bool:
        """Delete an item by URL."""
        item = self.get_by_url(url)
        if item:
            self.session.delete(item)
            self.session.commit()
            return True
        return False
    
    def close(self):
        """Close the session."""
        self.session.close()