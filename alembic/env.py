"""
Al-Mudeer Alembic Migration Environment
Supports both SQLite (development) and PostgreSQL (production)
"""

import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool, create_engine

from alembic import context

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# this is the Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# No target_metadata since we're not using SQLAlchemy ORM models
# If you add SQLAlchemy models in the future, import and set here:
# from models import Base
# target_metadata = Base.metadata
target_metadata = None


def get_url():
    """Get database URL from environment or config"""
    db_type = os.getenv("DB_TYPE", "sqlite").lower()
    
    if db_type == "postgresql":
        url = os.getenv("DATABASE_URL")
        if url:
            return url
        # Fallback to building URL from components
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        user = os.getenv("DB_USER", "almudeer")
        password = os.getenv("DB_PASSWORD", "")
        database = os.getenv("DB_NAME", "almudeer")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        # SQLite
        db_path = os.getenv("DATABASE_PATH", "almudeer.db")
        return f"sqlite:///./{db_path}"


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    url = get_url()
    
    # Create engine with URL
    connectable = create_engine(
        url,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
