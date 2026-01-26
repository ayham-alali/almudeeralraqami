from .core import (
    LicenseKeyValidation,
    LicenseKeyResponse,
    LicenseKeyCreate,
    MessageInput,
    AnalysisResult,
    ProcessingResponse,
    CRMEntryCreate,
    CRMEntry,
    CRMListResponse,
    HealthCheck
)
# We can also do `from .core import *` but explicit is better.
# However, to be extra safe with backward compat for ALL symbols (including imports unused in schemas.py if any), `from .core import *` matches the "module rename" semantics best.
from .core import *
