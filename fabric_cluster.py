from fabrictestbed_extensions.fablib.fablib import FablibManager as fablib_manager
from scheduler import WorkerNode, ClusterManager
 
SLICE_NAME = "cluster_job_manager"
NODE_IMAGE = "default_ubuntu_22"

DEFAULT_WORKER_SPECS = [
    {"name": "worker-large",  "cores": 2, "ram": 8,  "disk": 20},
    {"name": "worker-medium", "cores": 2, "ram": 6,  "disk": 20},
    {"name": "worker-small",  "cores": 2, "ram": 4,  "disk": 20},
]

def provide_fabric_cluster(fablib, cluster: ClusterManager, workers=None, site=None, slice_name = SLICE_NAME):
    if workers is None:
        workers = DEFAULT_WORKER_SPECS

    try:
        existing = fablib.get_slice(name=slice_name)
        if existing:
            print(f"[FABRIC] Slice '{slice_name}' already exists, reconnecting...")
            return get_existing_slice(fablib, cluster, workers=workers, slice_name=slice_name)
    except Exception as e:
        if "not found" not in str(e).lower():
            print(f"[FABRIC] Warning: slice lookup failed ({e}), attempting fresh provision")

    if site is None:
        site = fablib.get_random_site()
    print(f"[FABRIC] Provisioning {len(workers)} workers at site: {site}")

    slice_obj = fablib.new_slice(name=slice_name)
    for spec in workers:
        slice_obj.add_node(
            name=spec["name"],
            site=site,
            cores=spec["cores"],
            ram=spec["ram"],
            disk=spec.get("disk", 20),
            image=NODE_IMAGE,
        )
        print(f"[FABRIC]   Adding {spec['name']}: {spec['cores']} cores, "
              f"{spec['ram']}GB RAM")
        
    print("[FABRIC] Submitting slice (this takes ~2-3 minutes)...")
    slice_obj.submit()
    print("[FABRIC] Slice is Active.")

    failed_nodes = []
    for spec in workers:
        try:
            fabric_node = slice_obj.get_node(spec["name"])
            worker = WorkerNode(
                name=spec["name"],
                cores=spec["cores"],
                ram_mb=spec["ram"] * 1024,   # GB -> MB
                disk_gb=spec.get("disk", 20),
            )
            worker.fabric_node = fabric_node
            cluster.workers[spec["name"]] = worker
            print(f"[FABRIC]   {spec['name']} ready at {fabric_node.get_management_ip()}")
        except Exception as e:
            print(f"[FABRIC]   ERROR attaching {spec['name']}: {e}")
            failed_nodes.append(spec["name"])

    if failed_nodes:
        print(f"[FABRIC] WARNING: {len(failed_nodes)} node(s) failed to attach: {failed_nodes}")
    if not cluster.workers:
        raise RuntimeError("[FABRIC] No nodes attached — cluster unusable.")
    print(f"[FABRIC] Cluster ready: {list(cluster.workers.keys())}")
    return slice_obj

def get_existing_slice(fablib, cluster: ClusterManager, workers=None, slice_name=SLICE_NAME):

    if workers is None:
        workers = DEFAULT_WORKER_SPECS
 
    slice_obj = fablib.get_slice(name=slice_name)
    print(f"[FABRIC] Re-attached to existing slice '{slice_name}'")
 
    failed_nodes = []
    for spec in workers:
        try:
            fabric_node = slice_obj.get_node(spec["name"])
            worker = WorkerNode(
                name=spec["name"],
                cores=spec["cores"],
                ram_mb=spec["ram"] * 1024,
                disk_gb=spec.get("disk", 20),
            )
            worker.fabric_node = fabric_node
            cluster.workers[spec["name"]] = worker
            print(f"[FABRIC]   {spec['name']} at {fabric_node.get_management_ip()}")
        except Exception as e:
            print(f"[FABRIC]   ERROR attaching {spec['name']}: {e}")
            failed_nodes.append(spec["name"])

    if failed_nodes:
        print(f"[FABRIC] WARNING: {len(failed_nodes)} node(s) failed to attach: {failed_nodes}")
    if not cluster.workers:
        raise RuntimeError("[FABRIC] No nodes attached — cluster unusable.")
    return slice_obj

def teardown_fabric_cluster(fablib, slice_name=SLICE_NAME):
    try:
        slice_obj = fablib.get_slice(name=slice_name)
        slice_obj.delete()
        print(f"[FABRIC] Slice '{slice_name}' deleted successfully.")
    except Exception as e:
        print(f"[FABRIC] Could not delete slice '{slice_name}': {e}")
 