"""
Scheduler Module
Quota allocation, task decomposition, load balancing, and job-to-node mapping.
This prototype simulates worker nodes without FABRIC.
"""

from jobsubmission import JobDescription, JobSubmissionManager


class WorkerNode:

    def __init__(self, name, cores=2, ram_mb=4096, disk_gb=20):
        self.name = name
        self.fabric_node = None
        self.cores = cores
        self.ram_mb = ram_mb
        self.disk_gb = disk_gb
        self.allocated_cores = 0
        self.allocated_ram_mb = 0
        self.assigned_tasks = []
        self.status = "ready"

    def available_cores(self):
        return self.cores - self.allocated_cores

    def available_ram_mb(self):
        return self.ram_mb - self.allocated_ram_mb

    def utilization(self):
        if self.cores == 0:
            return 0.0
        return self.allocated_cores / self.cores

    def can_fit(self, cpus_needed, ram_needed):
        return (self.available_cores() >= cpus_needed and
                self.available_ram_mb() >= ram_needed)

    def allocate(self, cpus, ram_mb, subtask):
        self.allocated_cores += cpus
        self.allocated_ram_mb += ram_mb
        self.assigned_tasks.append(subtask)

    def deallocate(self, cpus, ram_mb, subtask):
        self.allocated_cores = max(0, self.allocated_cores - cpus)
        self.allocated_ram_mb = max(0, self.allocated_ram_mb - ram_mb)
        if subtask in self.assigned_tasks:
            self.assigned_tasks.remove(subtask)

    def __repr__(self):
        return (f"WorkerNode({self.name}, cores={self.cores}, "
                f"used={self.allocated_cores}, status={self.status})")


class SubTask:

    def __init__(self, parent_job_id, chunk_index, total_chunks, command,
                 cpus_needed=1, ram_needed=512):
        self.parent_job_id = parent_job_id
        self.chunk_index = chunk_index
        self.total_chunks = total_chunks
        self.command = command
        self.cpus_needed = cpus_needed
        self.ram_needed = ram_needed
        self.assigned_node = None
        self.status = "pending"
        self.result = None
        self.error = None

    def __repr__(self):
        return (f"SubTask(job={self.parent_job_id}, chunk={self.chunk_index}/"
                f"{self.total_chunks}, node={self.assigned_node}, status={self.status})")


class ClusterManager:

    def __init__(self):
        self.workers = {}

    def create_local_cluster(self, num_workers=3, cores_per_node=4, ram_per_node=4096):
        """Create simulated local worker nodes for prototyping."""
        for i in range(num_workers):
            name = f"worker-{i}"
            self.workers[name] = WorkerNode(name, cores=cores_per_node, ram_mb=ram_per_node)
        print(f"[CLUSTER] Created {num_workers} local workers with {cores_per_node} cores, {ram_per_node}MB RAM each")

    def get_available_workers(self):
        return [w for w in self.workers.values()
                if w.status == "ready" and w.available_cores() > 0]

    def get_cluster_status(self):
        status = {}
        for name, w in self.workers.items():
            status[name] = {
                "cores": w.cores,
                "cores_used": w.allocated_cores,
                "ram_mb": w.ram_mb,
                "ram_used_mb": w.allocated_ram_mb,
                "utilization": round(w.utilization() * 100, 1),
                "tasks": len(w.assigned_tasks),
                "status": w.status,
            }
        return status

    def total_available_cores(self):
        return sum(w.available_cores() for w in self.workers.values() if w.status == "ready")

    def total_available_ram(self):
        return sum(w.available_ram_mb() for w in self.workers.values() if w.status == "ready")

    def mark_node_down(self, node_name):
        if node_name in self.workers:
            self.workers[node_name].status = "unresponsive"
            print(f"[CLUSTER] Node {node_name} marked as unresponsive")

    def mark_node_ready(self, node_name):
        if node_name in self.workers:
            self.workers[node_name].status = "ready"


class Scheduler:

    def __init__(self, cluster: ClusterManager, job_manager: JobSubmissionManager):
        self.cluster = cluster
        self.job_manager = job_manager
        self.task_queue = []
        self.active_tasks = []
        self.completed_tasks = []
        self.max_quota_per_job = 0.5

    def calculate_quota(self, job: JobDescription):
        total_cores = sum(w.cores for w in self.cluster.workers.values())
        total_ram = sum(w.ram_mb for w in self.cluster.workers.values())
        queued_count = max(len(self.job_manager.get_queued_jobs()), 1)

        fair_cores = total_cores // queued_count
        fair_ram = total_ram // queued_count

        max_cores = int(total_cores * self.max_quota_per_job)
        max_ram = int(total_ram * self.max_quota_per_job)

        return {
            "max_cores": min(fair_cores, max_cores),
            "max_ram_mb": min(fair_ram, max_ram),
        }

    def check_resource_availability(self, job: JobDescription):
        needed_cpus = job.resources.get("cpus", 1)
        needed_ram = job.resources.get("memory_mb", 512)

        available_cores = self.cluster.total_available_cores()
        available_ram = self.cluster.total_available_ram()

        if available_cores < needed_cpus:
            return False, f"Not enough CPUs: need {needed_cpus}, available {available_cores}"
        if available_ram < needed_ram:
            return False, f"Not enough RAM: need {needed_ram}MB, available {available_ram}MB"
        return True, "Resources available"

    def decompose_job(self, job: JobDescription):
        cpus_per_chunk = max(1, job.resources.get("cpus", 1) // job.chunks)
        ram_per_chunk = max(256, job.resources.get("memory_mb", 512) // job.chunks)

        subtasks = []
        for i in range(job.chunks):
            cmd = f"{job.command} --chunk-index {i} --total-chunks {job.chunks}"
            st = SubTask(
                parent_job_id=job.job_id,
                chunk_index=i,
                total_chunks=job.chunks,
                command=cmd,
                cpus_needed=cpus_per_chunk,
                ram_needed=ram_per_chunk,
            )
            subtasks.append(st)
        return subtasks

    def select_node_for_task(self, subtask: SubTask):
        candidates = [w for w in self.cluster.get_available_workers()
                      if w.can_fit(subtask.cpus_needed, subtask.ram_needed)]
        if not candidates:
            return None
        candidates.sort(key=lambda w: w.utilization())
        return candidates[0]

    def assign_tasks_to_nodes(self, subtasks: list):
        assigned = []
        unassigned = []
        for st in subtasks:
            node = self.select_node_for_task(st)
            if node:
                node.allocate(st.cpus_needed, st.ram_needed, st)
                st.assigned_node = node.name
                st.status = "assigned"
                assigned.append(st)
            else:
                unassigned.append(st)
        return assigned, unassigned

    def schedule_job(self, job: JobDescription):
        available, reason = self.check_resource_availability(job)
        if not available:
            print(f"[SCHED] Cannot schedule {job.job_name}: {reason}")
            return False, reason

        quota = self.calculate_quota(job)
        print(f"[SCHED] Quota for {job.job_name}: {quota}")

        subtasks = self.decompose_job(job)
        print(f"[SCHED] Decomposed {job.job_name} into {len(subtasks)} sub-tasks")

        assigned, unassigned = self.assign_tasks_to_nodes(subtasks)

        if unassigned:
            self.task_queue.extend(unassigned)
            print(f"[SCHED] {len(unassigned)} sub-tasks queued (waiting for resources)")

        if assigned:
            self.active_tasks.extend(assigned)
            job.update_state("running")
            job.assigned_nodes = list(set(st.assigned_node for st in assigned))
            print(f"[SCHED] {job.job_name} running on nodes: {job.assigned_nodes}")
            return True, f"Scheduled {len(assigned)} sub-tasks, {len(unassigned)} queued"

        return False, "No resources available for any sub-task"

    def schedule_all_queued(self):
        results = []
        for job in self.job_manager.get_queued_jobs():
            success, msg = self.schedule_job(job)
            results.append((job.job_id, success, msg))
        return results

    def release_resources(self, subtask: SubTask):
        if subtask.assigned_node and subtask.assigned_node in self.cluster.workers:
            node = self.cluster.workers[subtask.assigned_node]
            node.deallocate(subtask.cpus_needed, subtask.ram_needed, subtask)
        if subtask in self.active_tasks:
            self.active_tasks.remove(subtask)
        self.completed_tasks.append(subtask)

    def handle_job_completion(self, job_id: str):
        job = self.job_manager.get_job(job_id)
        job_tasks = [t for t in self.completed_tasks if t.parent_job_id == job_id]
        all_done = len(job_tasks) == job.chunks
        all_ok = all(t.status == "completed" for t in job_tasks)

        if all_done and all_ok:
            job.result = [t.result for t in job_tasks]
            job.update_state("completed")
            print(f"[SCHED] Job {job.job_name} completed successfully")
            return True
        return False

    def handle_task_failure(self, subtask: SubTask, error: str):
        subtask.status = "failed"
        subtask.error = error
        self.release_resources(subtask)

        node = self.select_node_for_task(subtask)
        if node:
            subtask.status = "pending"
            subtask.error = None
            subtask.assigned_node = node.name
            node.allocate(subtask.cpus_needed, subtask.ram_needed, subtask)
            subtask.status = "assigned"
            self.active_tasks.append(subtask)
            print(f"[SCHED] Rescheduled failed sub-task to {node.name}")
            return True
        else:
            job = self.job_manager.get_job(subtask.parent_job_id)
            job.update_state("failed", error=f"Sub-task {subtask.chunk_index} failed: {error}")
            print(f"[SCHED] Job {job.job_name} failed: no node available for reschedule")
            return False
