"""
In-memory async job manager for long-running tasks (storytelling, PDF export).
Jobs are tracked by ID with status and progress updates.
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    id: str
    status: JobStatus = JobStatus.QUEUED
    progress: int = 0
    total_steps: int = 0
    current_step: str = ""
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "status": self.status.value,
            "progress": self.progress,
            "total_steps": self.total_steps,
            "current_step": self.current_step,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class JobManager:
    def __init__(self):
        self._jobs: dict[str, Job] = {}
        self._files: dict[str, bytes] = {}

    def create_job(self, total_steps: int = 1) -> Job:
        job = Job(id=str(uuid.uuid4())[:12], total_steps=total_steps)
        self._jobs[job.id] = job
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def update_progress(self, job_id: str, progress: int, step: str):
        job = self._jobs.get(job_id)
        if job:
            job.progress = progress
            job.current_step = step
            job.status = JobStatus.PROCESSING
            job.updated_at = datetime.now(timezone.utc)

    def complete_job(self, job_id: str, file_bytes: bytes):
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.COMPLETED
            job.progress = job.total_steps
            job.current_step = "Concluído"
            job.updated_at = datetime.now(timezone.utc)
            self._files[job_id] = file_bytes

    def fail_job(self, job_id: str, error: str):
        job = self._jobs.get(job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error = error
            job.updated_at = datetime.now(timezone.utc)

    def get_file(self, job_id: str) -> Optional[bytes]:
        return self._files.get(job_id)

    def cleanup_old_jobs(self, max_age_minutes: int = 30):
        now = datetime.now(timezone.utc)
        expired = [
            jid for jid, j in self._jobs.items()
            if (now - j.created_at).total_seconds() > max_age_minutes * 60
        ]
        for jid in expired:
            self._jobs.pop(jid, None)
            self._files.pop(jid, None)


# Singleton
job_manager = JobManager()
