"""
Job Submission Module
Handles parsing, validation, and management of job description files (YAML/JSON).
"""

import yaml
import json
import uuid
import os
from datetime import datetime


class JobDescription:
    """Represents a validated job submitted by a user."""

    REQUIRED_FIELDS = ["job_name", "command"]
    VALID_STATES = ["queued", "running", "completed", "failed", "rescheduled"]

    def __init__(self, job_name, command, resources=None, time_limit=None,
                 distributable=False, chunks=1):
        self.job_id = str(uuid.uuid4())[:8]
        self.job_name = job_name
        self.command = command
        self.resources = resources or {"cpus": 1, "memory_mb": 512}
        self.time_limit = time_limit  # in seconds, None means no limit
        self.distributable = distributable
        self.chunks = chunks if distributable else 1
        self.state = "queued"
        self.submitted_at = datetime.now().isoformat()
        self.started_at = None
        self.completed_at = None
        self.assigned_nodes = []
        self.result = None
        self.error = None

    def to_dict(self):
        """Serialize job to dictionary."""
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "command": self.command,
            "resources": self.resources,
            "time_limit": self.time_limit,
            "distributable": self.distributable,
            "chunks": self.chunks,
            "state": self.state,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "assigned_nodes": self.assigned_nodes,
            "result": self.result,
            "error": self.error,
        }

    def update_state(self, new_state, error=None):
        """Update the job's state."""
        if new_state not in self.VALID_STATES:
            raise ValueError(f"Invalid state: {new_state}. Must be one of {self.VALID_STATES}")

        self.state = new_state

        if new_state == "running":
            self.started_at = datetime.now().isoformat()
        elif new_state in ("completed", "failed"):
            self.completed_at = datetime.now().isoformat()
        if error:
            self.error = error

    def __repr__(self):
        return f"Job({self.job_id}, {self.job_name}, state={self.state})"


class JobValidator:
    """Validates job descriptions before they enter the queue."""

    MAX_CPUS = 16
    MAX_MEMORY_MB = 16384
    MAX_CHUNKS = 10
    MAX_TIME_LIMIT = 3600  # 1 hour

    @staticmethod
    def validate(job_data: dict) -> list:
        """
        Validate a job description dictionary.
        Returns a list of error messages. Empty list means valid.
        """
        errors = []

        # Check required fields
        for field in JobDescription.REQUIRED_FIELDS:
            if field not in job_data or not job_data[field]:
                errors.append(f"Missing required field: '{field}'")

        if errors:
            return errors  

        # Validate command
        if not isinstance(job_data["command"], str):
            errors.append("'command' must be a string")

        # Validate resources if provided
        resources = job_data.get("resources", {})
        if resources:
            cpus = resources.get("cpus", 1)
            memory = resources.get("memory_mb", 512)

            if not isinstance(cpus, int) or cpus < 1:
                errors.append(f"'cpus' must be a positive integer, got {cpus}")
            elif cpus > JobValidator.MAX_CPUS:
                errors.append(f"'cpus' exceeds max allowed ({JobValidator.MAX_CPUS})")

            if not isinstance(memory, (int, float)) or memory < 1:
                errors.append(f"'memory_mb' must be a positive number, got {memory}")
            elif memory > JobValidator.MAX_MEMORY_MB:
                errors.append(f"'memory_mb' exceeds max allowed ({JobValidator.MAX_MEMORY_MB})")

        # Validate time limit
        time_limit = job_data.get("time_limit")
        if time_limit is not None:
            if not isinstance(time_limit, (int, float)) or time_limit <= 0:
                errors.append(f"'time_limit' must be a positive number, got {time_limit}")
            elif time_limit > JobValidator.MAX_TIME_LIMIT:
                errors.append(f"'time_limit' exceeds max allowed ({JobValidator.MAX_TIME_LIMIT}s)")

        # Validate distribution settings
        distributable = job_data.get("distributable", False)
        chunks = job_data.get("chunks", 1)

        if distributable:
            if not isinstance(chunks, int) or chunks < 2:
                errors.append("'chunks' must be an integer >= 2 when job is distributable")
            elif chunks > JobValidator.MAX_CHUNKS:
                errors.append(f"'chunks' exceeds max allowed ({JobValidator.MAX_CHUNKS})")

        return errors


class JobSubmissionManager:
    """Handles loading, validating, and queuing job descriptions."""

    def __init__(self):
        self.jobs = {}  # job_id -> JobDescription

    def load_from_file(self, filepath: str) -> dict:
        """
        Load a job description from a YAML or JSON file.
        Returns the raw dictionary.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Job file not found: {filepath}")

        ext = os.path.splitext(filepath)[1].lower()

        with open(filepath, "r") as f:
            if ext in (".yaml", ".yml"):
                data = yaml.safe_load(f)
            elif ext == ".json":
                data = json.load(f)
            else:
                raise ValueError(f"Unsupported file format: {ext}. Use .yaml, .yml, or .json")

        return data

    def submit(self, job_data: dict) -> JobDescription:
        """
        Validate and submit a job from a dictionary.
        Returns the JobDescription if valid, raises ValueError if not.
        """
        errors = JobValidator.validate(job_data)
        if errors:
            raise ValueError(f"Job validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

        job = JobDescription(
            job_name=job_data["job_name"],
            command=job_data["command"],
            resources=job_data.get("resources"),
            time_limit=job_data.get("time_limit"),
            distributable=job_data.get("distributable", False),
            chunks=job_data.get("chunks", 1),
        )

        self.jobs[job.job_id] = job
        print(f"[SUBMITTED] {job}")
        return job

    def submit_from_file(self, filepath: str) -> JobDescription:
        """Load a job from file and submit it."""
        job_data = self.load_from_file(filepath)
        return self.submit(job_data)

    def get_job(self, job_id: str) -> JobDescription:
        """Retrieve a job by ID."""
        if job_id not in self.jobs:
            raise KeyError(f"Job not found: {job_id}")
        return self.jobs[job_id]

    def get_all_jobs(self) -> list:
        """Return all jobs as a list of dictionaries."""
        return [job.to_dict() for job in self.jobs.values()]

    def get_jobs_by_state(self, state: str) -> list:
        """Return all jobs with a given state."""
        return [job for job in self.jobs.values() if job.state == state]

    def get_queued_jobs(self) -> list:
        """Return all jobs waiting to be scheduled."""
        return self.get_jobs_by_state("queued")

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued job. Returns True if cancelled, False if not cancellable."""
        job = self.get_job(job_id)
        if job.state == "queued":
            job.update_state("failed", error="Cancelled by user")
            print(f"[CANCELLED] {job}")
            return True
        else:
            print(f"[WARN] Cannot cancel job {job_id} in state '{job.state}'")
            return False


if __name__ == "__main__":
    manager = JobSubmissionManager()

    # Test 1: Submit a valid simple job
    print("=== Test 1: Simple job ===")
    job1 = manager.submit({
        "job_name": "hello_world",
        "command": "echo Hello World",
    })
    print(job1.to_dict())

    # Test 2: Submit a distributable job
    print("\n=== Test 2: Distributable job ===")
    job2 = manager.submit({
        "job_name": "matrix_multiply",
        "command": "python3 matrix_mult.py",
        "resources": {"cpus": 4, "memory_mb": 2048},
        "time_limit": 300,
        "distributable": True,
        "chunks": 3,
    })
    print(job2.to_dict())

    # Test 3: Submit an invalid job
    print("\n=== Test 3: Invalid job ===")
    try:
        manager.submit({
            "job_name": "",
            "command": "",
            "resources": {"cpus": -1, "memory_mb": 99999},
            "chunks": 50,
        })
    except ValueError as e:
        print(f"Caught expected error:\n{e}")

    # Test 4: List queued jobs
    print(f"\n=== Queued jobs: {len(manager.get_queued_jobs())} ===")
    for j in manager.get_queued_jobs():
        print(f"  {j}")