"""Test database connection."""

from src.database.manager import get_db_manager, get_db_session
from src.database.models import CrawledItem

def test_connection():
    print("Testing database connection...")
    
    # Initialize manager
    manager = get_db_manager()
    
    # Get session
    session = get_db_session()
    
    try:
        # Test query
        count = session.query(CrawledItem).count()
        print(f"[OK] Connected! Current items in database: {count}")
        
        # Test insert
        test_item = CrawledItem(
            url="https://test.com",
            title="Test Item",
            content="This is a test item.",
            domain="test.com",
            status_code=200,
            content_length=100
        )
        session.add(test_item)
        session.commit()
        print("[OK] Test item inserted successfully")
        
        # Clean up
        session.delete(test_item)
        session.commit()
        print("[OK] Test item removed")
        
    except Exception as e:
        print(f"[FAIL] Database error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    test_connection()