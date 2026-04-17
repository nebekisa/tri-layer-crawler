"""Create all database tables."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.manager import get_db_manager
from src.database.models import Base

print("Creating database tables...")
manager = get_db_manager()
print(f"Tables created: {list(Base.metadata.tables.keys())}")
print("Done!")