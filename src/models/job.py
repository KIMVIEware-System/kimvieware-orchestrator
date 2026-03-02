"""

Job Model for Orchestrator
"""
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum

class JobPhase(str, Enum):
    SUBMITTED = "submitted"
    VALIDATING = "validating"
    VALIDATED = "validated"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    REDUCING = "reducing"
    REDUCED = "reduced"
    OPTIMIZING = "optimizing"
    OPTIMIZED = "optimized"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"

class JobStatus(BaseModel):
    """Job status tracking"""
    job_id: str
    phase: JobPhase
    sut_name: str
    created_at: datetime
    updated_at: datetime
    
    # Phase-specific data
    phase0_data: Optional[Dict[str, Any]] = None  # Validation
    phase1_data: Optional[Dict[str, Any]] = None  # Extraction
    phase2_data: Optional[Dict[str, Any]] = None  # SGATS
    phase3_data: Optional[Dict[str, Any]] = None  # EvoPath-GA
    phase4_data: Optional[Dict[str, Any]] = None  # Execution
    
    error: Optional[str] = None

class JobSubmitResponse(BaseModel):
    """Response after submission"""
    job_id: str
    status: str
    message: str
    tracking_url: str
