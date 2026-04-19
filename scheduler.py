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

    def create_local_cluster(self, num_workers=3, cores_per_node=4,
                                ram_per_node=4096):
            specs = [
                (f"worker-large",  cores_per_node * 2, ram_per_node * 2),
                (f"worker-medium", cores_per_node,     ram_per_node),
                (f"worker-small",  max(1, cores_per_node // 2), ram_per_node // 2),
            ]
            for i in range(num_workers):
                name, cores, ram = specs[i % len(specs)]
                if num_workers > 3:
                    name = f"{name}-{i}"
                self.workers[name] = WorkerNode(name, cores=cores, ram_mb=ram)
            print(f"[CLUSTER] Created {num_workers} local workers (heterogeneous sizes)")

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

    def total_cores(self):
        return sum(w.cores for w in self.workers.values())

    def total_ram_mb(self):
        return sum(w.ram_mb for w in self.workers.values())

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

    def calculate_quotas(self):
        queued = self.job_manager.get_queued_jobs()
        if not queued:
            return {}

        total_cores = self.cluster.total_cores()
        total_ram = self.cluster.total_ram_mb()

        total_demanded_cores = sum(j.resources.get("cpus", 1) for j in queued)
        total_demanded_ram   = sum(j.resources.get("memory_mb", 512) for j in queued)

        max_cores = int(total_cores * self.max_quota_per_job)
        max_ram   = int(total_ram   * self.max_quota_per_job)
 
        quotas = {}
        for job in queued:
            demanded_cores = job.resources.get("cpus",1)
            demanded_ram = job.resources.get("memory_mb",512)

            weight_cores = demanded_cores / max(total_demanded_cores, 1)
            weight_ram = demanded_ram / max(total_demanded_ram, 1)

            allocated_cores = max(1, min(int(total_cores * weight_cores), max_cores))
            allocated_rams = max(256, min(int(total_ram * weight_ram), max_ram))

            quotas[job.job_id] = {
                "cores": allocated_cores,
                "ram_mb" : allocated_rams
            }

        return quotas

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

    def decompose_job(self, job: JobDescription, quota:dict):
        quota_cores = quota["cores"]
        quota_ram   = quota["ram_mb"]
 
        if job.distributable:
            num_chunks = min(job.chunks, len(self.cluster.get_available_workers()))
            num_chunks = max(1, num_chunks)
        else:
            num_chunks = 1
 
        max_node_cores  = max((w.cores for w in self.cluster.workers.values()), default=1)
        cores_per_chunk = min(max(1, quota_cores // num_chunks), max_node_cores)
        ram_per_chunk   = max(256, job.resources.get("memory_mb", 512) // num_chunks)
 
        subtasks = []
        for i in range(num_chunks):
            wrapped_cmd = (
                f"export CHUNK_INDEX={i} TOTAL_CHUNKS={num_chunks} "
                f"JOB_ID={job.job_id}; {job.command}"
            )
            st = SubTask(
                parent_job_id=job.job_id,
                chunk_index=i,
                total_chunks=num_chunks,
                command=wrapped_cmd,
                cpus_needed=cores_per_chunk,
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
        assigned   = []
        unassigned = []
 
        used_nodes_per_job = {}   
 
        for st in subtasks:
            job_id = st.parent_job_id
            used = used_nodes_per_job.setdefault(job_id, set())
 
            candidates = [
                w for w in self.cluster.get_available_workers()
                if w.can_fit(st.cpus_needed, st.ram_needed)
                and w.name not in used
            ]
            if not candidates:
                candidates = [
                    w for w in self.cluster.get_available_workers()
                    if w.can_fit(st.cpus_needed, st.ram_needed)
                ]
 
            if candidates:
                candidates.sort(key=lambda w: (w.utilization(), -w.available_cores()))
                node = candidates[0]
                node.allocate(st.cpus_needed, st.ram_needed, st)
                st.assigned_node = node.name
                st.status = "assigned"
                used.add(node.name)
                assigned.append(st)
            else:
                unassigned.append(st)
 
        return assigned, unassigned

    def schedule_job(self, job: JobDescription):
        available, reason = self.check_resource_availability(job)
        if not available:
            print(f"[SCHED] Cannot schedule {job.job_name}: {reason}")
            return 0, 0

        quotas = self.calculate_quotas()
        quota  = quotas.get(job.job_id, {"cores": job.resources.get("cpus", 1),
                                          "ram_mb": job.resources.get("memory_mb", 512)})
        print(f"[SCHED] Quota for {job.job_name}: {quota}")

        subtasks = self.decompose_job(job, quota)
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

        return len(assigned), len(unassigned)

    def schedule_all_queued(self):
        queued = self.job_manager.get_queued_jobs()
        if not queued:
            return []

        quotas = self.calculate_quotas()

        # Decompose all jobs into subtasks first — no allocation yet
        all_subtasks = []
        for job in queued:
            quota = quotas.get(job.job_id, {"cores": job.resources.get("cpus", 1),
                                             "ram_mb": job.resources.get("memory_mb", 512)})
            print(f"[SCHED] Quota for {job.job_name}: {quota}")
            subtasks = self.decompose_job(job, quota)
            print(f"[SCHED] Decomposed {job.job_name} into {len(subtasks)} sub-tasks")
            all_subtasks.extend(subtasks)

        # Joint assignment — all jobs compete for nodes simultaneously
        assigned, unassigned = self.assign_tasks_to_nodes(all_subtasks)

        results = []
        for job in queued:
            job_assigned   = [st for st in assigned   if st.parent_job_id == job.job_id]
            job_unassigned = [st for st in unassigned if st.parent_job_id == job.job_id]

            if job_unassigned:
                self.task_queue.extend(job_unassigned)
                print(f"[SCHED] {len(job_unassigned)} sub-task(s) queued for {job.job_name}")

            if job_assigned:
                self.active_tasks.extend(job_assigned)
                job.update_state("running")
                job.assigned_nodes = list(set(st.assigned_node for st in job_assigned))
                print(f"[SCHED] {job.job_name} running on: {job.assigned_nodes}")

            results.append((job.job_id, len(job_assigned), len(job_unassigned)))

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
        expected = job_tasks[0].total_chunks if job_tasks else job.chunks
        all_done = len(job_tasks) == expected
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
            # No node free right now — undo the completed_tasks entry and re-queue
            if subtask in self.completed_tasks:
                self.completed_tasks.remove(subtask)
            subtask.status = "pending"
            subtask.error = None
            subtask.assigned_node = None
            self.task_queue.append(subtask)
            job = self.job_manager.get_job(subtask.parent_job_id)
            job.update_state("queued")
            print(f"[SCHED] No node available for {subtask.parent_job_id}-{subtask.chunk_index}, re-queued")
            return False
