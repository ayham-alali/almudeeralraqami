from typing import List, Optional
from datetime import datetime
from db_helper import get_db, execute_sql, fetch_all, fetch_one, commit_db
from models.base import ID_PK, TIMESTAMP_NOW

async def init_tasks_table():
    """Initialize tasks table"""
    async with get_db() as db:
        await execute_sql(db, f"""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                license_key_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                is_completed BOOLEAN DEFAULT FALSE,
                due_date TIMESTAMP,
                priority TEXT DEFAULT 'medium',
                color BIGINT,
                sub_tasks TEXT,  -- JSON string
                created_at {TIMESTAMP_NOW},
                updated_at TIMESTAMP,
                synced_at TIMESTAMP,
                FOREIGN KEY (license_key_id) REFERENCES license_keys(id)
            )
        """)
        
        # Check for new columns and migrate if needed (SQLite specific check)
        try:
            await execute_sql(db, "ALTER TABLE tasks ADD COLUMN color INTEGER")
            print("Migrated tasks: added color")
        except Exception:
            pass
            
        try:
            await execute_sql(db, "ALTER TABLE tasks ADD COLUMN sub_tasks TEXT")
            print("Migrated tasks: added sub_tasks")
        except Exception:
            pass
        
        # Indexes for performance
        await execute_sql(db, """
            CREATE INDEX IF NOT EXISTS idx_tasks_license_completed
            ON tasks(license_key_id, is_completed)
        """)
        
        await commit_db(db)
        print("Tasks table initialized")

async def get_tasks(license_id: int) -> List[dict]:
    """Get all tasks for a license"""
    async with get_db() as db:
        rows = await fetch_all(db, """
            SELECT * FROM tasks 
            WHERE license_key_id = ?
            ORDER BY created_at DESC
        """, (license_id,))
        return [_parse_task_row(dict(row)) for row in rows]

async def get_task(license_id: int, task_id: str) -> Optional[dict]:
    """Get a specific task"""
    async with get_db() as db:
        row = await fetch_one(db, """
            SELECT * FROM tasks 
            WHERE license_key_id = ? AND id = ?
        """, (license_id, task_id))
        return _parse_task_row(dict(row)) if row else None

def _parse_task_row(row: dict) -> dict:
    """Helper to parse JSON fields"""
    import json
    if row.get('sub_tasks') and isinstance(row['sub_tasks'], str):
        try:
            row['sub_tasks'] = json.loads(row['sub_tasks'])
        except:
            row['sub_tasks'] = []
    elif not row.get('sub_tasks'):
        row['sub_tasks'] = []
    return row

async def create_task(license_id: int, task_data: dict) -> dict:
    """Create a new task"""
    async with get_db() as db:
        # Convert list to JSON string if needed
        import json
        sub_tasks_val = task_data.get('sub_tasks')
        if isinstance(sub_tasks_val, list):
            sub_tasks_val = json.dumps(sub_tasks_val)
            
        await execute_sql(db, """
            INSERT INTO tasks (
                id, license_key_id, title, description, is_completed, due_date, priority, color, sub_tasks, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (
            task_data['id'],
            license_id,
            task_data['title'],
            task_data.get('description'),
            task_data.get('is_completed', False),
            task_data.get('due_date'),
            task_data.get('priority', 'medium'),
            task_data.get('color'),
            sub_tasks_val
        ))
        await commit_db(db)
        return await get_task(license_id, task_data['id'])

async def update_task(license_id: int, task_id: str, task_data: dict) -> Optional[dict]:
    """Update a task"""
    fields = []
    values = []
    
    # helper to add field if present
    for key, val in task_data.items():
        if val is not None:
            fields.append(f"{key} = ?")
            values.append(val)
            
    if not fields:
        return await get_task(license_id, task_id)
        
    fields.append("updated_at = CURRENT_TIMESTAMP")
    values.append(license_id)
    values.append(task_id)
    
    query = f"UPDATE tasks SET {', '.join(fields)} WHERE license_key_id = ? AND id = ?"
    
    async with get_db() as db:
        await execute_sql(db, query, tuple(values))
        await commit_db(db)
        return await get_task(license_id, task_id)

async def delete_task(license_id: int, task_id: str) -> bool:
    """Delete a task"""
    async with get_db() as db:
        await execute_sql(db, """
            DELETE FROM tasks 
            WHERE license_key_id = ? AND id = ?
        """, (license_id, task_id))
        await commit_db(db)
        return True
