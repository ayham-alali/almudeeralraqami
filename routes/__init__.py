"""Al-Mudeer Routes Package"""

from .core_integrations import router as integrations_router
from .features import router as features_router
from .whatsapp import router as whatsapp_router
from .team import router as team_router
from .export import router as export_router
from .notifications import router as notifications_router

# Subscription router is imported directly in main.py to avoid circular imports
# from .subscription import router as subscription_router

__all__ = [
    'integrations_router', 
    'features_router',
    'whatsapp_router',
    'team_router',
    'export_router',
    'notifications_router'
]

