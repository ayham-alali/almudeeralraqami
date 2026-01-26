from fastapi import APIRouter, HTTPException, Depends
from typing import List
from schemas.tasks import TaskCreate, TaskResponse, TaskUpdate
from models.tasks import get_tasks, create_task, update_task, delete_task, get_task
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/tasks", tags=["Tasks"])

@router.get("/", response_model=List[TaskResponse])
async def list_tasks(license: dict = Depends(get_license_from_header)):
    """Get all tasks"""
    tasks = await get_tasks(license["license_id"])
    return tasks

@router.post("/", response_model=TaskResponse)
async def create_new_task(
    task: TaskCreate,
    license: dict = Depends(get_license_from_header)
):
    """Create or sync a task"""
    # Check if exists to determine insert/update (upsert logic for sync)
    existing = await get_task(license["license_id"], task.id)
    if existing:
        return await update_task(license["license_id"], task.id, task.model_dump(exclude_unset=True))
        
    result = await create_task(license["license_id"], task.model_dump())
    if not result:
        raise HTTPException(status_code=500, detail="Failed to create task")
    return result

@router.put("/{task_id}", response_model=TaskResponse)
async def update_existing_task(
    task_id: str,
    task: TaskUpdate,
    license: dict = Depends(get_license_from_header)
):
    """Update a task"""
    result = await update_task(license["license_id"], task_id, task.model_dump(exclude_unset=True))
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    return result

@router.delete("/{task_id}")
async def delete_existing_task(
    task_id: str,
    license: dict = Depends(get_license_from_header)
):
    """Delete a task"""
    success = await delete_task(license["license_id"], task_id)
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"success": True}
