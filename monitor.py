"""
Monitor Module
Tracks job progress, detects failures, timeouts, and node health.
"""

import time
from datetime import datetime
from jobsubmission import JobSubmissionManager
from scheduler import Scheduler, ClusterManager


class Monitor:

    def __init__(self, scheduler: Scheduler, cluster: ClusterManager,
                 job_manager: JobSubmissionManager):
        self.scheduler = scheduler
        self.cluster = cluster
        self.job_manager = job_manager
        self.logs = []

    def log(self, message):
        entry = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
        self.logs.append(entry)
        print(entry)

    def check_job_status(self, job_id: str):
        """Check the current state of a job and its sub-tasks."""
        job = self.job_manager.get_job(job_id)
        active = [t for t in self.scheduler.active_tasks if t.parent_job_id == job_id]
        completed = [t for t in self.scheduler.completed_tasks if t.parent_job_id == job_id]
        queued = [t for t in self.scheduler.task_queue if t.parent_job_id == job_id]

        return {
            "job_id": job_id,
            "job_name": job.job_name,
            "state": job.state,
            "total_chunks": job.chunks,
            "active_tasks": len(active),
            "completed_tasks": len(completed),
            "queued_tasks": len(queued),
            "assigned_nodes": job.assigned_nodes,
        }

    def check_node_health(self, ssh_timeout=10):
        newly_failed = []
 
        for name, worker in list(self.cluster.workers.items()):
            if worker.status == "unresponsive":
                continue    # already known bad
 
            fabric_node = getattr(worker, "fabric_node", None)
            if fabric_node is None:
                continue    # local/simulated — no probe needed
 
            try:
                stdout, stderr = fabric_node.execute(
                    "echo alive",
                    quiet=True,
                    timeout=ssh_timeout,
                )
                if "alive" not in stdout:
                    raise RuntimeError(
                        f"Unexpected probe response: {stdout!r}"
                    )
                # Node is healthy
            except Exception as e:
                self.log(f"Node {name} FAILED health probe: {e}")
                self.handle_node_failure(name)
                newly_failed.append(name)
 
        return newly_failed

    def handle_node_failure(self, node_name: str):
        """Handle a failed node by rescheduling its tasks."""
        self.cluster.mark_node_down(node_name)
        self.log(f"Handling failure of node {node_name}")

        affected = [t for t in self.scheduler.active_tasks
                    if t.assigned_node == node_name]

        rescheduled = 0
        failed = 0
        for task in affected:
            success = self.scheduler.handle_task_failure(task, f"Node {node_name} went down")
            if success:
                rescheduled += 1
            else:
                failed += 1

        self.log(f"Node {node_name}: {rescheduled} tasks rescheduled, {failed} re-queued")
        return rescheduled, failed

    def check_timeouts(self):
        """Check for jobs that have exceeded their time limit."""
        timed_out = []
        now = datetime.now()

        for job in self.job_manager.get_jobs_by_state("running"):
            if job.time_limit and job.started_at:
                started = datetime.fromisoformat(job.started_at)
                elapsed = (now - started).total_seconds()
                if elapsed > job.time_limit:
                    self.log(f"Job {job.job_name} timed out ({elapsed:.0f}s > {job.time_limit}s)")
                    job.update_state("failed", error=f"Timed out after {elapsed:.0f}s")
                    timed_out.append(job.job_id)

                    for task in self.scheduler.active_tasks[:]:
                        if task.parent_job_id == job.job_id:
                            self.scheduler.release_resources(task)

        return timed_out

    def get_cluster_summary(self):
        """Get a summary of the entire system state."""
        jobs = self.job_manager.get_all_jobs()
        return {
            "total_jobs": len(jobs),
            "queued": len([j for j in jobs if j["state"] == "queued"]),
            "running": len([j for j in jobs if j["state"] == "running"]),
            "completed": len([j for j in jobs if j["state"] == "completed"]),
            "failed": len([j for j in jobs if j["state"] == "failed"]),
            "active_tasks": len(self.scheduler.active_tasks),
            "queued_tasks": len(self.scheduler.task_queue),
            "cluster": self.cluster.get_cluster_status(),
        }

    def poll(self):
        """Single poll cycle: check node health and job timeouts.
        Returns (failed_nodes, timed_out_job_ids)."""
        failed_nodes = self.check_node_health()
        timed_out    = self.check_timeouts()
        return failed_nodes, timed_out

    def get_logs(self, last_n=20):
        return self.logs[-last_n:]
