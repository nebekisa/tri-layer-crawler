"""
Database connection and session management.
"""

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool, QueuePool
from src.core.config_loader import get_settings
from src.database.models import Base

# Import Base after engine creation to avoid circular imports
def _get_base():
    from src.database.models import Base
    return Base




class DatabaseManager:
    """
    Manages database connections and sessions.
    
    Supports:
        - SQLite for development (zero setup)
        - PostgreSQL for production (scalable, concurrent)
    """
    
    _instance = None
    _engine = None
    _SessionLocal = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _get_connection_string(self) -> str:
        """Build the appropriate connection string."""
        settings = get_settings()
        db_config = settings.database
        
        if db_config.type == "postgresql":
            # PostgreSQL connection string
            return (
                f"postgresql://{db_config.user}:{db_config.password}"
                f"@{db_config.host}:{db_config.port}/{db_config.name}"
            )
        else:
            # SQLite connection string (fallback)
            project_root = Path(__file__).parent.parent.parent
            db_path = project_root / settings.storage.database_path
            db_path.parent.mkdir(parents=True, exist_ok=True)
            return f"sqlite:///{db_path}"
    
    def _initialize(self):
        """Initialize database engine and create tables."""
        settings = get_settings()
        db_config = settings.database
        
        connection_string = self._get_connection_string()
        
        # Configure engine based on database type
        if db_config.type == "postgresql":
            self._engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=db_config.pool_size,
                max_overflow=db_config.max_overflow,
                echo=False  # Set to True for SQL debugging
            )
            print(f"[OK] PostgreSQL database connected: {db_config.host}:{db_config.port}/{db_config.name}")
        else:
            self._engine = create_engine(
                connection_string,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
                echo=False
            )
            print(f"[OK] SQLite database initialized: {settings.storage.database_path}")
        
        # Create session factory
        self._SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self._engine
        )
        
        # Create tables
        Base.metadata.create_all(bind=self._engine)
    
    def get_session(self) -> Session:
        """Get a new database session."""
        return self._SessionLocal()
    
    def close(self):
        """Close database connections."""
        if self._engine:
            self._engine.dispose()


# Global instance
_db_manager = None

def get_db_manager() -> DatabaseManager:
    """Get or create the database manager singleton."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def get_db_session() -> Session:
    """Get a database session."""
    return get_db_manager().get_session()

def get_db_session_safe():
    """Get database session or return None if unavailable."""
    try:
        return get_db_session()
    except Exception as e:
        logger.warning(f"Database unavailable: {e}")
        return None