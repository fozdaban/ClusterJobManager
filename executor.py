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
        self._lock = threading.Lock()

    def _subtask_id(self, subtask: SubTask) -> str:
        return f"{subtask.parent_job_id}-{subtask.chunk_index}"

    def run_subtask(self, subtask: SubTask, timeout=None):
        subtask.status = "running"
        sid   = self._subtask_id(subtask)
        start = time.time()
 
        worker      = self.cluster.workers.get(subtask.assigned_node)
        fabric_node = getattr(worker, "fabric_node", None) if worker else None
 
        print(f"[EXEC] {'SSH' if fabric_node else 'local'} "
              f"subtask {sid} on {subtask.assigned_node}")
 
        try:
            # Real FABRIC SSH execution
            if fabric_node is not None:
                try:
                    stdout, stderr = fabric_node.execute(
                        subtask.command,
                        quiet=True,
                    )
                    return_code = 0
                except Exception as ssh_err:
                    err_str = str(ssh_err)
                    # Distinguish timeouts from other SSH failures
                    if any(kw in type(ssh_err).__name__.lower() or kw in err_str.lower()
                           for kw in ("timeout", "timedout", "timed out")):
                        duration = time.time() - start
                        result = TaskResult(
                            subtask_id=sid,
                            stderr=f"Subtask timed out after {timeout}s (SSH)",
                            return_code=-1,
                            duration=round(duration, 3),
                        )
                        subtask.status = "failed"
                        subtask.error  = result.stderr
                        with self._lock:
                            self.results[sid] = result
                        print(f"[EXEC] {sid} SSH timeout after {result.duration}s")
                        return result
                    raise

            else:
                # Fallback local subprocess
                proc = subprocess.run(
                    subtask.command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                stdout      = proc.stdout.strip()
                stderr      = proc.stderr.strip()
                return_code = proc.returncode

            duration = time.time() - start
            result   = TaskResult(
                subtask_id=sid,
                stdout=stdout.strip() if stdout else "",
                stderr=stderr.strip() if stderr else "",
                return_code=return_code,
                duration=round(duration, 3),
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start
            result   = TaskResult(
                subtask_id=sid,
                stderr=f"Subtask timed out after {timeout}s",
                return_code=-1,
                duration=round(duration, 3),
            )

        except Exception as e:
            duration = time.time() - start
            result   = TaskResult(
                subtask_id=sid,
                stderr=str(e),
                return_code=-1,
                duration=round(duration, 3),
            )
 
        if result.success:
            subtask.status = "completed"
            subtask.result = result.stdout
        else:
            subtask.status = "failed"
            subtask.error  = result.stderr
 
        with self._lock:
            self.results[sid] = result
 
        print(f"[EXEC] {sid} finished in {result.duration}s "
              f"(rc={result.return_code})"
              + (f" ERROR: {result.stderr[:80]}" if not result.success else ""))
 
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
