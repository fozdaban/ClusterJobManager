# Distributed Cluster Job Manager

A distributed cluster job manager built on FABRIC that maps user submitted workloads to resources across multiple VMs. The system supports job scheduling with resource quotas, task decomposition across nodes, load balancing, and fault handling.

## Architecture

The system follows a manager-worker architecture. A central manager node maintains the job queue, makes scheduling decisions, and distributes sub-tasks across worker nodes. Workers execute assigned tasks and report results back to the manager.

## Modules

### Job Submission (`job_submission.py`)
Handles parsing and validation of job description files (YAML/JSON). Each job specifies the command to run, resource requirements (CPU, memory), an optional time limit, and whether the job can be split across multiple nodes.

### Scheduler (`scheduler.py`)
The core scheduling engine. Determines resource quotas for each job based on current cluster utilization, decomposes large jobs into sub-tasks that can be distributed across multiple workers, and performs load balancing to prevent any single node from being overloaded. Implements a scheduling policy that manages resource sharing when multiple jobs compete for the same pool of nodes.

### Executor (`executor.py`)
Responsible for deploying and running sub-tasks on remote worker nodes via SSH. Manages the communication between the manager and workers, collects results from distributed sub-tasks, and aggregates them into a final output.

### Monitor (`monitor.py`)
Tracks job progress across all worker nodes. Detects failures such as unresponsive nodes, exceeded time limits, or resource exhaustion. Updates job state (queued, running, completed, failed) and handles rescheduling of failed sub-tasks to healthy nodes.

### Persistence (`persistence.py`)
Stores job metadata, cluster state, and execution history. Maintains a record of all submitted jobs and their outcomes for status queries.

### User Interface (`ui.py`)
Provides a command-line and API-based interface for submitting jobs, querying job status, and viewing cluster state. 


## Job Description Format

Jobs are described in YAML files:

```yaml
job_name: matrix_multiply
command: python3 matrix_mult.py
resources:
  cpus: 4
  memory_mb: 2048
time_limit: 300
distributable: true
chunks: 3
```