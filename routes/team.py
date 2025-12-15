"""
Al-Mudeer - Team Management Routes
Multi-user support with role-based access control
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
import hashlib
import secrets

from models import (
    ROLES,
    create_team_member,
    get_team_members,
    get_team_member,
    get_team_member_by_email,
    update_team_member,
    delete_team_member,
    check_permission,
    log_team_activity,
    get_team_activity,
)
from dependencies import get_license_from_header

router = APIRouter(prefix="/api/team", tags=["Team"])


# ============ Schemas ============

class TeamMemberCreate(BaseModel):
    email: EmailStr
    name: str = Field(..., min_length=2, max_length=100)
    role: str = Field(default="agent")


class TeamMemberUpdate(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


class TeamMemberInvite(BaseModel):
    email: EmailStr
    name: str
    role: str = "agent"


# ============ Utility Functions ============

def hash_password(password: str) -> str:
    """Hash a password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()


def generate_temp_password() -> str:
    """Generate a temporary password"""
    return secrets.token_urlsafe(12)


# ============ Routes ============

@router.get("/roles")
async def list_roles():
    """Get available team roles and their permissions"""
    return {
        "roles": [
            {
                "id": role_id,
                "name": role_data["name"],
                "permissions": role_data["permissions"]
            }
            for role_id, role_data in ROLES.items()
        ]
    }


@router.get("/members")
async def list_team_members(license: dict = Depends(get_license_from_header)):
    """Get all team members"""
    members = await get_team_members(license["license_id"])
    
    # Add role names
    for member in members:
        member["role_name"] = ROLES.get(member["role"], {}).get("name", member["role"])
    
    return {"members": members, "total": len(members)}


@router.post("/members")
async def add_team_member(
    member: TeamMemberCreate,
    license: dict = Depends(get_license_from_header)
):
    """Add a new team member"""
    # Check if member already exists
    existing = await get_team_member_by_email(license["license_id"], member.email)
    if existing:
        raise HTTPException(status_code=400, detail="هذا البريد الإلكتروني مسجل مسبقاً")
    
    # Validate role
    if member.role not in ROLES:
        raise HTTPException(status_code=400, detail="الدور غير صالح")
    
    # Generate temporary password
    temp_password = generate_temp_password()
    password_hash = hash_password(temp_password)
    
    member_id = await create_team_member(
        license_id=license["license_id"],
        email=member.email,
        name=member.name,
        role=member.role,
        password_hash=password_hash
    )
    
    # Log activity
    await log_team_activity(
        license_id=license["license_id"],
        member_id=None,  # System action
        action="member_added",
        details=f"تمت إضافة {member.name} كـ {ROLES[member.role]['name']}"
    )
    
    return {
        "success": True,
        "message": f"تمت إضافة {member.name} بنجاح",
        "member_id": member_id,
        "temporary_password": temp_password,  # Send to user via email in production
        "note": "يرجى إرسال كلمة المرور المؤقتة للمستخدم عبر البريد الإلكتروني"
    }


@router.get("/members/{member_id}")
async def get_member_detail(
    member_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Get team member details"""
    member = await get_team_member(license["license_id"], member_id)
    if not member:
        raise HTTPException(status_code=404, detail="العضو غير موجود")
    
    # Don't expose password hash
    member.pop("password_hash", None)
    member["role_name"] = ROLES.get(member["role"], {}).get("name", member["role"])
    
    return {"member": member}


@router.patch("/members/{member_id}")
async def update_member(
    member_id: int,
    data: TeamMemberUpdate,
    license: dict = Depends(get_license_from_header)
):
    """Update team member"""
    member = await get_team_member(license["license_id"], member_id)
    if not member:
        raise HTTPException(status_code=404, detail="العضو غير موجود")
    
    # Can't change owner role
    if member["role"] == "owner" and data.role and data.role != "owner":
        raise HTTPException(status_code=400, detail="لا يمكن تغيير دور المالك")
    
    # Validate new role
    if data.role and data.role not in ROLES:
        raise HTTPException(status_code=400, detail="الدور غير صالح")
    
    await update_team_member(
        license["license_id"],
        member_id,
        **data.dict(exclude_none=True)
    )
    
    # Log activity
    changes = []
    if data.name:
        changes.append(f"الاسم: {data.name}")
    if data.role:
        changes.append(f"الدور: {ROLES[data.role]['name']}")
    if data.is_active is not None:
        changes.append("فعّال" if data.is_active else "معطّل")
    
    await log_team_activity(
        license_id=license["license_id"],
        member_id=None,
        action="member_updated",
        details=f"تم تحديث {member['name']}: {', '.join(changes)}"
    )
    
    return {"success": True, "message": "تم تحديث بيانات العضو"}


@router.delete("/members/{member_id}")
async def remove_member(
    member_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Remove team member"""
    member = await get_team_member(license["license_id"], member_id)
    if not member:
        raise HTTPException(status_code=404, detail="العضو غير موجود")
    
    # Can't delete owner
    if member["role"] == "owner":
        raise HTTPException(status_code=400, detail="لا يمكن حذف المالك")
    
    await delete_team_member(license["license_id"], member_id)
    
    # Log activity
    await log_team_activity(
        license_id=license["license_id"],
        member_id=None,
        action="member_removed",
        details=f"تم حذف {member['name']}"
    )
    
    return {"success": True, "message": "تم حذف العضو بنجاح"}


@router.post("/members/{member_id}/reset-password")
async def reset_member_password(
    member_id: int,
    license: dict = Depends(get_license_from_header)
):
    """Reset team member password"""
    member = await get_team_member(license["license_id"], member_id)
    if not member:
        raise HTTPException(status_code=404, detail="العضو غير موجود")
    
    # Generate new temporary password
    temp_password = generate_temp_password()
    password_hash = hash_password(temp_password)
    
    import aiosqlite
    from models import DATABASE_PATH
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE team_members SET password_hash = ? WHERE id = ?",
            (password_hash, member_id)
        )
        await db.commit()
    
    return {
        "success": True,
        "message": "تم إعادة تعيين كلمة المرور",
        "temporary_password": temp_password
    }


@router.get("/activity")
async def get_activity_log(
    limit: int = 50,
    license: dict = Depends(get_license_from_header)
):
    """Get team activity log"""
    activity = await get_team_activity(license["license_id"], limit)
    return {"activity": activity, "total": len(activity)}


@router.post("/invite")
async def invite_member(
    invite: TeamMemberInvite,
    license: dict = Depends(get_license_from_header)
):
    """Send invitation to a new team member"""
    # Check if already exists
    existing = await get_team_member_by_email(license["license_id"], invite.email)
    if existing:
        raise HTTPException(status_code=400, detail="هذا البريد الإلكتروني مسجل مسبقاً")
    
    # Generate temporary password and invite token
    temp_password = generate_temp_password()
    password_hash = hash_password(temp_password)
    
    member_id = await create_team_member(
        license_id=license["license_id"],
        email=invite.email,
        name=invite.name,
        role=invite.role,
        password_hash=password_hash
    )
    
    # In production, send email here
    # For now, return the credentials
    
    return {
        "success": True,
        "message": f"تم إرسال دعوة إلى {invite.email}",
        "member_id": member_id,
        "credentials": {
            "email": invite.email,
            "temporary_password": temp_password
        },
        "note": "في الإنتاج، سيتم إرسال هذه المعلومات عبر البريد الإلكتروني"
    }

