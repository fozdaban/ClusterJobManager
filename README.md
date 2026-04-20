# Distributed Cluster Job Manager

A distributed cluster job manager built on FABRIC that maps user-submitted workloads to resources across multiple VMs. The system supports job scheduling with resource quotas, task decomposition across nodes, load balancing, and fault handling.

## Architecture

The system follows a manager-worker architecture. A central manager node maintains the job queue, makes scheduling decisions, and distributes sub-tasks across worker nodes. Workers execute assigned tasks and report results back to the manager.

## Modules

- **`jobsubmission.py`** — Handles parsing and validation of job description files (YAML/JSON). Each job specifies the command to run, resource requirements (CPU, memory), an optional time limit, and whether the job can be split across multiple nodes.
- **`scheduler.py`** — Core scheduling engine. Determines resource quotas, decomposes large jobs into sub-tasks, and performs load balancing to prevent any single node from being overloaded.
- **`executor.py`** — Runs sub-tasks on worker nodes as local subprocesses with parallel threading. Will use SSH-based remote execution when deployed on FABRIC.
- **`monitor.py`** — Tracks job progress across all worker nodes. Detects failures (unresponsive nodes, exceeded time limits, resource exhaustion) and reschedules failed sub-tasks to healthy nodes.
- **`persistence.py`** — Stores job metadata, cluster state, and execution history for status queries.
- **`interface.py`** — Jupyter notebook widget interface for submitting jobs, viewing a job status dashboard, inspecting job details, and monitoring cluster state.
- **`fabric_cluster.py`** — Provisions and manages the FABRIC slice. Creates worker nodes on a remote site, reconnects to existing slices, and tears down infrastructure when done.
- **`system.py`** — Central coordinator that wires all components together and exposes a clean API used by the demo notebook (provision, run demos, show UI, teardown).

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

## Dependencies

- Python 3.x
- PyYAML
- ipywidgets


Demo Video: https://www.loom.com/share/d9629a6892f64234a23232bf1775c36f
