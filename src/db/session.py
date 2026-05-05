"""Database session factory."""
from collections.abc import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings
from src.db.models import Base


def _get_engine():
    settings = get_settings()
    kwargs = {}
    if "postgresql" in settings.database_url:
        kwargs = {"pool_size": 5, "max_overflow": 10, "pool_pre_ping": True}
    return create_engine(settings.database_url, echo=False, **kwargs)


engine = _get_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_db_connection() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
