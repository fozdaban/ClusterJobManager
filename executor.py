"""
Executor Module
Runs sub-tasks on worker nodes. This prototype uses subprocess instead of SSH.
"""

import subprocess
import threading
import time
from scheduler import SubTask, ClusterManager


class TaskResult:

    def __init__(self, subtask_id, stdout="", stderr="", return_code=-1, duration=0):
        self.subtask_id = subtask_id
        self.stdout = stdout
        self.stderr = stderr
        self.return_code = return_code
        self.duration = duration
        self.success = return_code == 0


class Executor:

    def __init__(self, cluster: ClusterManager):
        self.cluster = cluster
        self.results = {}

    def run_subtask(self, subtask: SubTask, timeout=None):
        """Execute a single sub-task locally as a subprocess."""
        subtask.status = "running"
        print(f"[EXEC] Running {subtask} on {subtask.assigned_node}")

        start = time.time()
        try:
            proc = subprocess.run(
                subtask.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            duration = time.time() - start

            result = TaskResult(
                subtask_id=f"{subtask.parent_job_id}-{subtask.chunk_index}",
                stdout=proc.stdout.strip(),
                stderr=proc.stderr.strip(),
                return_code=proc.returncode,
                duration=round(duration, 3),
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start
            result = TaskResult(
                subtask_id=f"{subtask.parent_job_id}-{subtask.chunk_index}",
                stderr="Task timed out",
                return_code=-1,
                duration=round(duration, 3),
            )

        except Exception as e:
            duration = time.time() - start
            result = TaskResult(
                subtask_id=f"{subtask.parent_job_id}-{subtask.chunk_index}",
                stderr=str(e),
                return_code=-1,
                duration=round(duration, 3),
            )

        if result.success:
            subtask.status = "completed"
            subtask.result = result.stdout
        else:
            subtask.status = "failed"
            subtask.error = result.stderr

        self.results[result.subtask_id] = result
        print(f"[EXEC] {subtask} finished in {result.duration}s (code={result.return_code})")
        return result

    def run_subtasks_parallel(self, subtasks: list, timeout=None):
        """Run multiple sub-tasks in parallel using threads."""
        threads = []
        for st in subtasks:
            t = threading.Thread(target=self.run_subtask, args=(st, timeout))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return [self.results.get(f"{st.parent_job_id}-{st.chunk_index}") for st in subtasks]

    def run_job_subtasks(self, subtasks: list, timeout=None):
        """Run all sub-tasks for a job. Uses parallel execution if multiple."""
        if len(subtasks) == 1:
            return [self.run_subtask(subtasks[0], timeout)]
        return self.run_subtasks_parallel(subtasks, timeout)
