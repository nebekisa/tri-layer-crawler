"""
CSV Repository for data access.
"""

import csv
from pathlib import Path
from typing import List, Dict, Optional

from src.core.config_loader import get_settings


class CsvRepository:
    """Reads crawled data from CSV file."""
    
    def __init__(self):
        settings = get_settings()
        project_root = Path(__file__).parent.parent.parent
        self.csv_path = project_root / settings.storage.csv_output_path
    
    def read_all(self) -> List[Dict]:
        """Read all items from CSV."""
        if not self.csv_path.exists():
            return []
        
        items = []
        with open(self.csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                items.append(dict(row))
        
        return items
    
    def find_by_url(self, url: str) -> Optional[Dict]:
        """Find a specific item by URL."""
        items = self.read_all()
        for item in items:
            if item.get('url') == url:
                return item
        return None
    
    def count(self) -> int:
        """Return total number of items."""
        return len(self.read_all())