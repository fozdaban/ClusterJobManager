"""
Persistence Module

Stores job metadata and execution history to disk as JSON.
Allows state to survive kernel restarts on JupyterHub.
"""

import json
import os
from datetime import datetime


class Persistence:
    def __init__(self, filepath="job_history.json"):
        self.filepath = filepath
        self._records = self._load()

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        return []

    def _save(self):
        with open(self.filepath, "w") as f:
            json.dump(self._records, f, indent=2, default=str)

    def record_job(self, job_dict: dict):
        """Upsert a job record (insert or update by job_id)."""
        for i, r in enumerate(self._records):
            if r.get("job_id") == job_dict.get("job_id"):
                self._records[i] = job_dict
                self._save()
                return
        self._records.append(job_dict)
        self._save()

    def get_history(self):
        """Return all recorded jobs."""
        return list(self._records)

    def clear(self):
        self._records = []
        self._save()

    def snapshot(self, job_manager):
        """Persist all jobs from a JobSubmissionManager."""
        for job in job_manager.get_all_jobs():
            self.record_job(job)
        print(f"[PERSIST] Saved {len(self._records)} job records to {self.filepath}")