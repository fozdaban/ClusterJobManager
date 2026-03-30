"""
Scheduler Module
Quota allocation, task decomposition, load balancing, and job-to-node mapping.
"""

from fabrictestbed_extensions.fablib.fablib import FablibManager
from jobsubmission import JobDescription, JobSubmissionManager


class WorkerNode:

    def __init__(self, name, fabric_node=None):
        self.name = name
        self.fabric_node = fabric_node
        self.cores = 0
        self.ram_mb = 0
        self.disk_gb = 0
        self.allocated_cores = 0
        self.allocated_ram_mb = 0
        self.assigned_tasks = []
        self.status = "unknown"

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

    def __repr__(self):
        return (f"WorkerNode({self.name}, cores={self.cores}, "
                f"allocated={self.allocated_cores}, status={self.status})")


class SubTask:

    def __init__(self, parent_job_id, chunk_index, total_chunks, command):
        self.parent_job_id = parent_job_id
        self.chunk_index = chunk_index
        self.total_chunks = total_chunks
        self.command = command
        self.assigned_node = None
        self.status = "pending"
        self.result = None
        self.error = None

    def __repr__(self):
        return (f"SubTask(job={self.parent_job_id}, chunk={self.chunk_index}/"
                f"{self.total_chunks}, status={self.status})")


class ClusterManager:

    def __init__(self):
        self.fablib = None
        self.slice = None
        self.workers = {}

    def initialize_fablib(self):
        # TBD
        pass

    def create_slice(self, slice_name, num_workers=3, cores_per_node=2,
                     ram_per_node=8, disk_per_node=10, site=None):
        # TBD
        pass

    def load_existing_slice(self, slice_name):
        # TBD
        pass

    def _register_workers(self):
        # TBD
        pass

    def get_available_workers(self):
        # TBD
        pass

    def get_cluster_status(self):
        # TBD
        pass

    def teardown_slice(self):
        # TBD
        pass


class Scheduler:

    def __init__(self, cluster: ClusterManager, job_manager: JobSubmissionManager):
        self.cluster = cluster
        self.job_manager = job_manager
        self.task_queue = []
        self.active_tasks = []
        self.max_quota_per_job = 0.5

    def calculate_quota(self, job: JobDescription):
        # TBD
        pass

    def check_resource_availability(self, job: JobDescription):
        # TBD
        pass

    def decompose_job(self, job: JobDescription):
        # TBD
        pass

    def select_node_for_task(self, subtask: SubTask, cpus_needed, ram_needed):
        # TBD
        pass

    def assign_tasks_to_nodes(self, subtasks: list):
        # TBD
        pass

    def schedule_job(self, job: JobDescription):
        # TBD
        pass

    def schedule_all_queued(self):
        # TBD
        pass

    def release_resources(self, subtask: SubTask):
        # TBD
        pass

    def handle_job_completion(self, job_id: str):
        # TBD
        pass

    def handle_task_failure(self, subtask: SubTask, error: str):
        # TBD
        pass