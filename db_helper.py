"""
Database Helper - Unified interface for SQLite and PostgreSQL
"""

import os
from contextlib import asynccontextmanager

DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DATABASE_PATH = os.getenv("DATABASE_PATH", "almudeer.db")
DATABASE_URL = os.getenv("DATABASE_URL")

if DB_TYPE == "postgresql":
    try:
        import asyncpg
        POSTGRES_AVAILABLE = True
    except ImportError:
        raise ImportError("PostgreSQL selected but asyncpg not installed. Install with: pip install asyncpg")
else:
    import aiosqlite
    POSTGRES_AVAILABLE = False


@asynccontextmanager
async def get_db():
    """Get database connection context manager"""
    if DB_TYPE == "postgresql" and POSTGRES_AVAILABLE:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is required for PostgreSQL")
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            yield conn
        finally:
            await conn.close()
    else:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            yield db


def adapt_sql_for_db(sql: str) -> str:
    """Adapt SQL syntax for current database type"""
    if DB_TYPE == "postgresql":
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        sql = sql.replace("AUTOINCREMENT", "")
        sql = sql.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT NOW()")
    return sql


async def execute_sql(db, sql: str, params=None):
    """Execute SQL with proper parameter handling"""
    sql = adapt_sql_for_db(sql)
    if DB_TYPE == "postgresql":
        # Convert SQLite-style ? placeholders to $1, $2, ... for asyncpg
        if params:
            sql = _convert_sql_params(sql, params)
            return await db.execute(sql, *params)
        else:
            return await db.execute(sql)
    else:
        if params:
            return await db.execute(sql, params)
        else:
            return await db.execute(sql)


def _convert_sql_params(sql: str, params: list) -> str:
    """Convert SQLite ? placeholders to PostgreSQL $1, $2, etc."""
    if DB_TYPE == "postgresql" and params:
        # Replace ? with $1, $2, etc.
        param_index = 1
        result = ""
        i = 0
        while i < len(sql):
            if sql[i] == '?' and (i == 0 or sql[i-1] != "'"):
                result += f"${param_index}"
                param_index += 1
            else:
                result += sql[i]
            i += 1
        return result
    return sql


async def fetch_all(db, sql: str, params=None):
    """Fetch all rows"""
    sql = adapt_sql_for_db(sql)
    if DB_TYPE == "postgresql":
        if params:
            sql = _convert_sql_params(sql, params)
            rows = await db.fetch(sql, *params)
        else:
            rows = await db.fetch(sql)
        return [dict(row) for row in rows]
    else:
        if params:
            cursor = await db.execute(sql, params)
        else:
            cursor = await db.execute(sql)
        rows = await cursor.fetchall()
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        return []


async def fetch_one(db, sql: str, params=None):
    """Fetch one row"""
    sql = adapt_sql_for_db(sql)
    if DB_TYPE == "postgresql":
        if params:
            sql = _convert_sql_params(sql, params)
            row = await db.fetchrow(sql, *params)
        else:
            row = await db.fetchrow(sql)
        return dict(row) if row else None
    else:
        if params:
            cursor = await db.execute(sql, params)
        else:
            cursor = await db.execute(sql, params)
        row = await cursor.fetchone()
        if row and cursor.description:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None


async def commit_db(db):
    """Commit database transaction"""
    if DB_TYPE != "postgresql":
        await db.commit()

