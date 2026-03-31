"""
Persistence Module
Stores job metadata, cluster state, and execution history.
Uses JSON files for the local prototype.
"""

import json
import os
from datetime import datetime
from jobsubmission import JobSubmissionManager


STORAGE_DIR = "job_data"


class Persistence:

    def __init__(self, storage_dir=STORAGE_DIR):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def _filepath(self, filename):
        return os.path.join(self.storage_dir, filename)

    def save_jobs(self, job_manager: JobSubmissionManager):
        """Save all jobs to a JSON file."""
        data = job_manager.get_all_jobs()
        with open(self._filepath("jobs.json"), "w") as f:
            json.dump(data, f, indent=2)
        print(f"[PERSIST] Saved {len(data)} jobs")

    def load_jobs(self, job_manager: JobSubmissionManager):
        """Load jobs from JSON file back into the job manager."""
        path = self._filepath("jobs.json")
        if not os.path.exists(path):
            print("[PERSIST] No saved jobs found")
            return

        with open(path, "r") as f:
            data = json.load(f)

        count = 0
        for job_data in data:
            if job_data["job_id"] not in job_manager.jobs:
                from job_submission import JobDescription
                job = JobDescription(
                    job_name=job_data["job_name"],
                    command=job_data["command"],
                    resources=job_data.get("resources"),
                    time_limit=job_data.get("time_limit"),
                    distributable=job_data.get("distributable", False),
                    chunks=job_data.get("chunks", 1),
                )
                job.job_id = job_data["job_id"]
                job.state = job_data["state"]
                job.submitted_at = job_data["submitted_at"]
                job.started_at = job_data.get("started_at")
                job.completed_at = job_data.get("completed_at")
                job.assigned_nodes = job_data.get("assigned_nodes", [])
                job.result = job_data.get("result")
                job.error = job_data.get("error")
                job_manager.jobs[job.job_id] = job
                count += 1

        print(f"[PERSIST] Loaded {count} jobs")

    def save_cluster_state(self, cluster_status: dict):
        """Save cluster state snapshot."""
        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "nodes": cluster_status,
        }
        with open(self._filepath("cluster_state.json"), "w") as f:
            json.dump(snapshot, f, indent=2)

    def save_logs(self, logs: list):
        """Append logs to a log file."""
        with open(self._filepath("monitor.log"), "a") as f:
            for entry in logs:
                f.write(entry + "\n")

    def load_logs(self):
        """Read saved logs."""
        path = self._filepath("monitor.log")
        if not os.path.exists(path):
            return []
        with open(path, "r") as f:
            return f.read().strip().split("\n")

    def clear(self):
        """Delete all saved data."""
        for fname in os.listdir(self.storage_dir):
            os.remove(self._filepath(fname))
        print("[PERSIST] Cleared all saved data")
