"""
system.py

Central coordinator for the Cluster Job Manager.
Wires all components together and exposes clean methods
that the demo notebook calls directly — one line per action.

Usage (in notebook):
    from system import ClusterSystem
    system = ClusterSystem()
    system.provision(fablib, site="GPN")
    system.demo_quota_and_distribution()
    system.demo_timeout()
    system.demo_node_failure()
    system.show_ui()
    system.teardown(fablib)
"""

import threading
import time

from fabric_cluster import (
    provide_fabric_cluster,
    get_existing_slice,
    teardown_fabric_cluster,
    DEFAULT_WORKER_SPECS,
)
from jobsubmission import JobSubmissionManager
from scheduler import ClusterManager, Scheduler
from executor import Executor
from monitor import Monitor
from persistence import Persistence


class ClusterSystem:
    """
    Owns all system components. The notebook never constructs
    JobSubmissionManager, Scheduler, Executor, or Monitor directly —
    it just calls methods on this object.
    """

    def __init__(self):
        self.cluster     = ClusterManager()
        self.fabric_slice = None
        self._reset_managers()

    def _reset_managers(self):
        """Create fresh job/scheduler/executor/monitor instances."""
        self.job_manager = JobSubmissionManager()
        self.scheduler   = Scheduler(self.cluster, self.job_manager)
        self.executor    = Executor(self.cluster)
        self.monitor     = Monitor(self.scheduler, self.cluster, self.job_manager)
        self.persistence = Persistence("job_history.json")

    def _reset_cluster_state(self):
        """Clear all allocations on worker nodes (between demos)."""
        for w in self.cluster.workers.values():
            w.status          = "ready"
            w.allocated_cores = 0
            w.allocated_ram_mb = 0
            w.assigned_tasks  = []

    # Provisioning

    def provision(self, fablib, workers=None, site=None,
                  slice_name="cluster-job-manager"):
        """
        Provision a heterogeneous FABRIC cluster and wire it in.
        Call this once at the start of the notebook session.

        Parameters
       -------
        fablib      : FablibManager (created in notebook Cell 1)
        workers     : list of dicts {name, cores, ram, disk}
                      Defaults to DEFAULT_WORKER_SPECS (8/4/2 core nodes)
        site        : FABRIC site name, e.g. "GPN". Random if None.
        slice_name  : Name for the FABRIC slice.
        """
        self.fabric_slice = provide_fabric_cluster(
            fablib, self.cluster,
            workers=workers,
            site=site,
            slice_name=slice_name,
        )
        self._reset_managers()
        return self.fabric_slice

    def reconnect(self, fablib, workers=None,
                  slice_name="cluster-job-manager"):
        """
        Re-attach to an existing FABRIC slice without re-provisioning.
        Use after a kernel restart when the slice is still alive.
        """
        self.fabric_slice = get_existing_slice(
            fablib, self.cluster,
            workers=workers,
            slice_name=slice_name,
        )
        self._reset_managers()
        return self.fabric_slice

    def teardown(self, fablib, slice_name="cluster-job-manager"):
        """Delete the FABRIC slice and release all VMs."""
        teardown_fabric_cluster(fablib, slice_name=slice_name)
        self.fabric_slice = None


    # Local testing (no FABRIC)

    def use_local_cluster(self):
        """
        Create a simulated local cluster for offline testing.
        Skips FABRIC provisioning entirely.
        """
        self.cluster.create_local_cluster()
        self._reset_managers()
        print("[SYSTEM] Using local simulated cluster (no FABRIC)")

    # Cluster inspection

    def show_cluster(self):
        """Print current cluster node specs and allocation state."""
        print(f"{'Node':<20} {'Cores':>6} {'Used':>6} {'RAM MB':>8} "
              f"{'RAM Used':>10} {'Util%':>7} {'Tasks':>6} {'Status':<12}")
        print("-" * 80)
        for name, info in self.cluster.get_cluster_status().items():
            print(f"{name:<20} {info['cores']:>6} {info['cores_used']:>6} "
                  f"{info['ram_mb']:>8} {info['ram_used_mb']:>10} "
                  f"{info['utilization']:>7} {info['tasks']:>6} "
                  f"{info['status']:<12}")

    def show_quotas(self):
        """
        Compute and print the fair-share quotas for all currently
        queued jobs without actually scheduling them.
        """
        quotas = self.scheduler.calculate_quotas()
        if not quotas:
            print("No queued jobs.")
            return
        total_cores = self.cluster.total_cores()
        total_ram   = self.cluster.total_ram_mb()
        print(f"Cluster total: {total_cores} cores, {total_ram} MB RAM")
        print(f"{'Job':<20} {'Demanded':>10} {'Quota Cores':>12} "
              f"{'Quota RAM MB':>14} {'Share%':>8}")
        print("-" * 70)
        for job_id, q in quotas.items():
            job      = self.job_manager.get_job(job_id)
            demanded = job.resources.get("cpus", 1)
            share    = round(q["cores"] / total_cores * 100, 1)
            print(f"{job.job_name:<20} {demanded:>10} {q['cores']:>12} "
                  f"{q['ram_mb']:>14} {share:>7}%")

    def show_jobs(self):
        """Print all jobs and their current states."""
        jobs = self.job_manager.get_all_jobs()
        if not jobs:
            print("No jobs submitted.")
            return
        print(f"{'ID':<10} {'Name':<20} {'State':<12} {'Chunks':>7} "
              f"{'Nodes':<30} {'Error'}")
        print("-" * 90)
        for j in jobs:
            nodes = ", ".join(j["assigned_nodes"]) if j["assigned_nodes"] else "-"
            err   = (j["error"] or "")[:40]
            print(f"{j['job_id']:<10} {j['job_name']:<20} {j['state']:<12} "
                  f"{j['chunks']:>7} {nodes:<30} {err}")

    def show_monitor_logs(self, last_n=30):
        """Print the last N monitor log entries."""
        logs = self.monitor.get_logs(last_n=last_n)
        if not logs:
            print("No monitor logs yet.")
            return
        for entry in logs:
            print(entry)

    # Internal execution helper

    def _run_and_release(self, subtask):
        job = self.job_manager.get_job(subtask.parent_job_id)
        self.executor.run_subtask(subtask, timeout=job.time_limit)
        self.scheduler.release_resources(subtask)
        self.scheduler.handle_job_completion(subtask.parent_job_id)
        self.persistence.record_job(job.to_dict())
        self._dispatch_queued_subtasks()

    def _dispatch_queued_subtasks(self):
        still_waiting = []
        for subtask in list(self.scheduler.task_queue):
            node = self.scheduler.select_node_for_task(subtask)
            if node:
                node.allocate(subtask.cpus_needed, subtask.ram_needed, subtask)
                subtask.assigned_node = node.name
                subtask.status = "assigned"
                self.scheduler.active_tasks.append(subtask)
                print(f"[SYSTEM] Dispatched queued subtask {subtask.parent_job_id}-{subtask.chunk_index} to {node.name}")
                job = self.job_manager.get_job(subtask.parent_job_id)
                job.update_state("running")
                job.assigned_nodes = list(set((job.assigned_nodes or []) + [node.name]))
                t = threading.Thread(target=self._run_and_release,
                                     args=(subtask,), daemon=True)
                t.start()
            else:
                still_waiting.append(subtask)
        self.scheduler.task_queue = still_waiting

    def _run_active_subtasks(self):
        """
        Launch all currently scheduled subtasks in parallel threads.
        Each thread SSHes into its assigned FABRIC VM via executor.
        Blocks until all threads complete.
        Returns wall-clock elapsed time.
        """
        subtasks = list(self.scheduler.active_tasks)
        if not subtasks:
            print("[SYSTEM] No active subtasks to run.")
            return 0.0

        node_set = set(st.assigned_node for st in subtasks)
        print(f"[SYSTEM] Launching {len(subtasks)} subtask(s) across "
              f"{len(node_set)} node(s): {node_set}")

        wall_start = time.time()

        threads = [
            threading.Thread(target=self._run_and_release, args=(st,), daemon=True)
            for st in subtasks
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        elapsed = round(time.time() - wall_start, 1)
        print(f"[SYSTEM] All subtasks finished in {elapsed}s wall-clock time.")
        return elapsed

    # Demo A — quota-based scheduling + distributed execution

    def demo_quota_and_distribution(self):
        """
        Demo A: weighted fair-share quota + distributed parallel execution.

        Submits three jobs with different resource demands, shows computed
        quotas, schedules them, runs chunks in parallel across FABRIC VMs,
        and prints wall-clock time as proof of distribution.
        """
        self._reset_cluster_state()
        self._reset_managers()

        print("=" * 60)
        print("DEMO A — Quota-based scheduling + distributed execution")
        print("=" * 60)

        # Submit jobs with deliberately different resource demands
        job_heavy = self.job_manager.submit({
            "job_name":      "heavy_job",
            "command":       "sleep 20",
            "resources":     {"cpus": 6, "memory_mb": 6144},
            "time_limit":    120,
            "distributable": True,
            "chunks":        3,
        })
        job_medium = self.job_manager.submit({
            "job_name":      "medium_job",
            "command":       "sleep 15",
            "resources":     {"cpus": 3, "memory_mb": 3072},
            "time_limit":    90,
            "distributable": True,
            "chunks":        2,
        })
        job_light = self.job_manager.submit({
            "job_name":      "light_job",
            "command":       "sleep 10",
            "resources":     {"cpus": 1, "memory_mb": 512},
            "time_limit":    60,
            "distributable": False,
        })

        # Show quotas before scheduling
        print("\nWeighted fair-share quotas (computed over all jobs together):")
        self.show_quotas()

        # Schedule all jobs simultaneously
        print("\nScheduling all jobs...")
        results = self.scheduler.schedule_all_queued()
        for job_id, assigned, waiting in results:
            job = self.job_manager.get_job(job_id)
            print(f"  {job.job_name}: {assigned} subtask(s) → "
                  f"{job.assigned_nodes}, {waiting} queued")

        print("\nCluster utilization after scheduling:")
        self.show_cluster()

        # Execute in parallel
        print("\nExecuting subtasks in parallel (real SSH to FABRIC VMs)...")
        elapsed = self._run_active_subtasks()

        # Results
        print("\nJob results:")
        self.show_jobs()
        total_sequential = sum(
            j.resources.get("cpus", 1) * 10
            for j in [job_heavy, job_medium, job_light]
        )
        print(f"\nWall-clock time:  {elapsed}s")
        print(f"Sequential would: ~45s (3 jobs × ~15s avg, single-threaded)")
        print("Parallelism is working." if elapsed < 35 else
              "Note: distribution may have been limited by cluster capacity.")

        return {
            "heavy":  self.job_manager.get_job(job_heavy.job_id).state,
            "medium": self.job_manager.get_job(job_medium.job_id).state,
            "light":  self.job_manager.get_job(job_light.job_id).state,
            "wall_clock_s": elapsed,
        }

    # Demo B — timeout detection

    def demo_timeout(self, poll_interval=5, polls=8):
        """
        Demo B: job timeout detection via monitor polling.

        Submits a job whose time_limit is shorter than its command duration.
        Polls the monitor every poll_interval seconds. The monitor detects
        the exceeded deadline and marks the job failed.

        Parameters
       -------
        poll_interval : seconds between monitor polls
        polls         : number of poll cycles to run
        """
        self._reset_cluster_state()
        self._reset_managers()

        print("=" * 60)
        print("DEMO B — Timeout error detection")
        print("=" * 60)

        # Will sleep 999s but time_limit is 15s
        job_timeout = self.job_manager.submit({
            "job_name":  "timeout_job",
            "command":   "sleep 999",
            "resources": {"cpus": 1, "memory_mb": 512},
            "time_limit": 15,
        })
        # Normal job that should complete fine
        job_ok = self.job_manager.submit({
            "job_name":  "ok_job",
            "command":   "sleep 5 && echo done",
            "resources": {"cpus": 1, "memory_mb": 512},
            "time_limit": 30,
        })

        self.scheduler.schedule_all_queued()

        # Launch subtasks in background threads
        subtasks = list(self.scheduler.active_tasks)

        threads = [
            threading.Thread(target=self._run_and_release, args=(st,), daemon=True)
            for st in subtasks
        ]
        for t in threads:
            t.start()

        print(f"\nPolling monitor every {poll_interval}s for timeout detection...")
        for poll in range(polls):
            time.sleep(poll_interval)
            failed_nodes, timed_out = self.monitor.poll()
            status_line = (
                f"  Poll {poll+1:>2}: "
                f"timed_out={timed_out}, failed_nodes={failed_nodes}"
            )
            print(status_line)
            if timed_out:
                print(f"           ↳ Timeout caught for job(s): {timed_out}")
            # Stop early if both jobs are resolved
            running = self.job_manager.get_jobs_by_state("running")
            if not running:
                break

        for t in threads:
            t.join(timeout=2)

        print("\nFinal job states:")
        self.show_jobs()
        print("\nMonitor logs:")
        self.show_monitor_logs(last_n=10)

    # Demo C — node failure + rescheduling

    def demo_node_failure(self, fail_node="worker-small",
                          failure_delay=10, poll_interval=5, polls=10):
        """
        Demo C: unresponsive node detection and task rescheduling.

        Submits several jobs, then simulates a node going down after
        failure_delay seconds. The monitor detects the failure, marks
        the node unresponsive, and reschedules affected tasks.

        Parameters
       -------
        fail_node     : name of the worker to simulate failing
        failure_delay : seconds to wait before injecting the failure
        poll_interval : seconds between monitor polls
        polls         : max poll cycles
        """
        self._reset_cluster_state()
        self._reset_managers()

        print("=" * 60)
        print("DEMO C — Node failure detection + task rescheduling")
        print("=" * 60)

        # Submit 3 jobs — they'll be spread across nodes
        jobs = []
        for i in range(3):
            j = self.job_manager.submit({
                "job_name":  f"node_fail_job_{i}",
                "command":   "sleep 40",
                "resources": {"cpus": 2, "memory_mb": 1024},
                "time_limit": 120,
            })
            jobs.append(j)

        self.scheduler.schedule_all_queued()

        print("\nInitial assignment:")
        for st in self.scheduler.active_tasks:
            job = self.job_manager.get_job(st.parent_job_id)
            print(f"  {job.job_name} → {st.assigned_node}")

        # Launch all subtasks in background
        subtasks = list(self.scheduler.active_tasks)

        threads = [
            threading.Thread(target=self._run_and_release, args=(st,), daemon=True)
            for st in subtasks
        ]
        for t in threads:
            t.start()

        # Wait, then inject node failure
        print(f"\nJobs running... injecting {fail_node} failure "
              f"in {failure_delay}s")
        time.sleep(failure_delay)

        print(f"\n>>> Simulating {fail_node} going unresponsive <<<")
        rescheduled, perm_failed = self.monitor.handle_node_failure(fail_node)
        print(f"    Rescheduled: {rescheduled} task(s), "
              f"permanently failed: {perm_failed} task(s)")

        print("\nCluster state after failure:")
        self.show_cluster()

        # Continue polling — extend limit when a re-queued job is still running
        print(f"\nPolling every {poll_interval}s...")
        max_polls = polls * 3
        for poll in range(max_polls):
            time.sleep(poll_interval)
            failed_nodes, timed_out = self.monitor.poll()
            running = len(self.job_manager.get_jobs_by_state("running"))
            queued  = len(self.job_manager.get_jobs_by_state("queued"))
            done    = len([j for j in self.job_manager.get_all_jobs()
                           if j["state"] in ("completed", "failed")])
            print(f"  Poll {poll+1:>2}: running={running}, queued={queued}, done={done}, "
                  f"failed_nodes={failed_nodes}, timeouts={timed_out}")
            if running == 0 and queued == 0:
                break

        for t in threads:
            t.join(timeout=2)

        print("\nFinal job states:")
        self.show_jobs()
        print("\nMonitor logs:")
        self.show_monitor_logs(last_n=15)

    # Widget UI

    def show_ui(self):
        """Launch the full interactive ipywidgets UI."""
        from interface import NotebookUI
        self._reset_cluster_state()
        self._reset_managers()
        self.ui = NotebookUI(
            job_manager=self.job_manager,
            cluster=self.cluster,
            scheduler=self.scheduler,
            executor=self.executor,
            monitor=self.monitor,
            persistence=self.persistence,
        )
        self.ui.start_polling(interval=5)
        self.ui.display_all()
