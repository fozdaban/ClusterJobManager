"""
User Interface Module

Jupyter notebook widget interface for the cluster job manager.
Updated to use schedule_all_queued() + poll-based monitoring.
"""

import threading
import time

import ipywidgets as widgets
from IPython.display import display, HTML

from jobsubmission import JobSubmissionManager
from scheduler import ClusterManager, Scheduler
from executor import Executor
from monitor import Monitor
from persistence import Persistence


class NotebookUI:
    def __init__(self, job_manager=None, cluster=None, scheduler=None,
                 executor=None, monitor=None, persistence=None):
        self.job_manager = job_manager or JobSubmissionManager()
        self.cluster     = cluster     or ClusterManager()
        self.scheduler   = scheduler   or Scheduler(self.cluster, self.job_manager)
        self.executor    = executor    or Executor(self.cluster)
        self.monitor     = monitor     or Monitor(self.scheduler, self.cluster,
                                                   self.job_manager)
        self.persistence = persistence or Persistence()
        self._poll_thread = None
        self._polling     = False

    # Polling loop (background thread)

    def start_polling(self, interval=5):
        """Start background health+timeout polling."""
        if self._poll_thread and self._poll_thread.is_alive():
            return
        self._polling = True

        def _loop():
            while self._polling:
                self.monitor.poll()
                time.sleep(interval)

        self._poll_thread = threading.Thread(target=_loop, daemon=True)
        self._poll_thread.start()
        print(f"[UI] Background polling started (every {interval}s)")

    def stop_polling(self):
        self._polling = False
        print("[UI] Background polling stopped")

    # Widget builders

    def build_submission_form(self):
        self.job_name_input   = widgets.Text(description="Job Name:",
                                             placeholder="e.g. my_job")
        self.command_input    = widgets.Text(description="Command:",
                                             placeholder="e.g. sleep 30")
        self.cpus_input       = widgets.IntSlider(description="CPUs:",
                                                  min=1, max=16, value=2)
        self.memory_input     = widgets.IntSlider(description="Memory (MB):",
                                                  min=512, max=16384,
                                                  value=1024, step=512)
        self.time_limit_input = widgets.IntText(description="Time Limit (s):",
                                                value=120)
        self.dist_toggle      = widgets.Checkbox(description="Distributable",
                                                 value=False)
        self.chunks_input     = widgets.IntSlider(description="Chunks:",
                                                  min=1, max=10, value=2)
        self.submit_btn       = widgets.Button(description="Submit Job",
                                              button_style="success",
                                              icon="check")
        self.submit_out       = widgets.Output()
        self.submit_btn.on_click(self._on_submit)

        return widgets.VBox([
            widgets.HTML("<h3>Submit a Job</h3>"),
            self.job_name_input, self.command_input,
            self.cpus_input, self.memory_input,
            self.time_limit_input, self.dist_toggle, self.chunks_input,
            self.submit_btn, self.submit_out,
        ])

    def build_file_upload_widget(self):
        self.file_upload  = widgets.FileUpload(accept=".yaml,.yml,.json",
                                               multiple=False,
                                               description="Upload Job File")
        self.upload_btn   = widgets.Button(description="Submit File",
                                           button_style="info", icon="upload")
        self.upload_out   = widgets.Output()
        self.upload_btn.on_click(self._on_file_upload)

        return widgets.VBox([
            widgets.HTML("<h3>Upload Job File</h3>"),
            self.file_upload, self.upload_btn, self.upload_out,
        ])

    def build_run_all_widget(self):
        """Button to schedule + execute all queued jobs at once."""
        self.run_all_btn = widgets.Button(description="Schedule & Run All",
                                          button_style="warning",
                                          icon="play")
        self.run_all_out = widgets.Output()
        self.run_all_btn.on_click(self._on_run_all)

        return widgets.VBox([
            widgets.HTML("<h3>Run All Queued Jobs</h3>"),
            widgets.HTML(
                "<p>Computes fair-share quotas across all queued jobs, "
                "decomposes and distributes them across the cluster, "
                "then executes in parallel.</p>"
            ),
            self.run_all_btn, self.run_all_out,
        ])

    def build_status_dashboard(self):
        self.refresh_btn = widgets.Button(description="Refresh",
                                          button_style="primary",
                                          icon="refresh")
        self.status_out  = widgets.Output()
        self.refresh_btn.on_click(self._on_refresh)

        return widgets.VBox([
            widgets.HTML("<h3>Job Status Dashboard</h3>"),
            self.refresh_btn, self.status_out,
        ])

    def build_job_detail_view(self):
        self.detail_id_input = widgets.Text(description="Job ID:",
                                            placeholder="e.g. 1b292728")
        self.detail_btn      = widgets.Button(description="Get Details",
                                              button_style="warning",
                                              icon="search")
        self.detail_out      = widgets.Output()
        self.detail_btn.on_click(self._on_detail)

        return widgets.VBox([
            widgets.HTML("<h3>Job Details</h3>"),
            widgets.HBox([self.detail_id_input, self.detail_btn]),
            self.detail_out,
        ])

    def build_cancel_widget(self):
        self.cancel_id_input = widgets.Text(description="Job ID:",
                                            placeholder="e.g. 1b292728")
        self.cancel_btn      = widgets.Button(description="Cancel Job",
                                              button_style="danger",
                                              icon="times")
        self.cancel_out      = widgets.Output()
        self.cancel_btn.on_click(self._on_cancel)

        return widgets.VBox([
            widgets.HTML("<h3>Cancel a Queued Job</h3>"),
            widgets.HBox([self.cancel_id_input, self.cancel_btn]),
            self.cancel_out,
        ])

    def build_cluster_view(self):
        self.cluster_refresh_btn = widgets.Button(description="Refresh",
                                                  button_style="primary",
                                                  icon="refresh")
        self.cluster_out         = widgets.Output()
        self.cluster_refresh_btn.on_click(self._on_cluster_refresh)

        return widgets.VBox([
            widgets.HTML("<h3>Cluster Status</h3>"),
            self.cluster_refresh_btn, self.cluster_out,
        ])

    def build_logs_view(self):
        self.logs_refresh_btn = widgets.Button(description="Refresh Logs",
                                               button_style="primary",
                                               icon="refresh")
        self.logs_out         = widgets.Output()
        self.logs_refresh_btn.on_click(self._on_logs_refresh)

        return widgets.VBox([
            widgets.HTML("<h3>Event Logs &amp; Task Output</h3>"),
            self.logs_refresh_btn, self.logs_out,
        ])

    def display_all(self):
        tabs = widgets.Tab()
        tabs.children = [
            self.build_submission_form(),
            self.build_file_upload_widget(),
            self.build_run_all_widget(),
            self.build_status_dashboard(),
            self.build_job_detail_view(),
            self.build_cancel_widget(),
            self.build_cluster_view(),
            self.build_logs_view(),
        ]
        for i, title in enumerate([
            "Submit Job", "Upload File", "Run All",
            "Dashboard", "Job Details", "Cancel Job",
            "Cluster", "Logs & Output"
        ]):
            tabs.set_title(i, title)
        display(tabs)

    # Event handlers

    def _on_submit(self, _button):
        self.submit_out.clear_output()
        with self.submit_out:
            try:
                job = self.job_manager.submit({
                    "job_name":     self.job_name_input.value,
                    "command":      self.command_input.value,
                    "resources":    {"cpus":      self.cpus_input.value,
                                     "memory_mb": self.memory_input.value},
                    "time_limit":   self.time_limit_input.value,
                    "distributable": self.dist_toggle.value,
                    "chunks":       self.chunks_input.value,
                })
                print(f"Job queued: {job.job_id} ({job.job_name})")
                print("Use 'Run All' tab to schedule and execute.")
            except ValueError as e:
                print(f"Validation error:\n{e}")

    def _on_file_upload(self, _button):
        self.upload_out.clear_output()
        with self.upload_out:
            try:
                import yaml, json as _json
                uploaded  = self.file_upload.value
                if not uploaded:
                    print("No file selected.")
                    return
                file_info = list(uploaded.values())[0] \
                    if isinstance(uploaded, dict) else uploaded[0]
                content = (file_info["content"]
                           if isinstance(file_info, dict)
                           else file_info.content)
                name    = (file_info.get("metadata", {}).get("name")
                           or file_info.get("name")
                           if isinstance(file_info, dict)
                           else file_info.name)
                if isinstance(content, memoryview):
                    content = bytes(content)
                text    = content.decode("utf-8") \
                    if isinstance(content, bytes) else content
                data    = (yaml.safe_load(text)
                           if name.endswith((".yaml", ".yml"))
                           else _json.loads(text))
                jobs = self.job_manager.submit_many(data)
                for job in jobs:
                    print(f"Job queued: {job.job_id} ({job.job_name})")
                print(f"{len(jobs)} job(s) queued. Use 'Run All' tab to execute.")
            except Exception as e:
                print(f"Error: {e}")

    def _on_run_all(self, _button):
        self.run_all_out.clear_output()
        with self.run_all_out:
            queued = self.job_manager.get_queued_jobs()
            if not queued:
                print("No queued jobs.")
                return

            print(f"Scheduling {len(queued)} queued job(s)...")
            results = self.scheduler.schedule_all_queued()
            for job_id, assigned, waiting in results:
                job = self.job_manager.get_job(job_id)
                print(f"  {job.job_name}: {assigned} subtask(s) assigned "
                      f"to {job.assigned_nodes}, {waiting} waiting")

            all_active = list(self.scheduler.active_tasks)
            if not all_active:
                print("No subtasks could be assigned.")
                return

            print(f"\nExecuting {len(all_active)} subtask(s) in parallel...")

            def _run_and_release(subtask):
                with self.run_all_out:
                    job     = self.job_manager.get_job(subtask.parent_job_id)
                    timeout = job.time_limit
                    self.executor.run_subtask(subtask, timeout=timeout)
                    self.scheduler.release_resources(subtask)
                    self.scheduler.handle_job_completion(subtask.parent_job_id)
                    self.persistence.record_job(job.to_dict())

            threads = [
                threading.Thread(target=_run_and_release, args=(st,), daemon=True)
                for st in all_active
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            print("\nAll subtasks finished.")
            for job_id, _, _ in results:
                job = self.job_manager.get_job(job_id)
                print(f"  {job.job_name}: {job.state}"
                      + (f" — {job.error}" if job.error else ""))

    def _on_refresh(self, _button):
        self.status_out.clear_output()
        with self.status_out:
            jobs = self.job_manager.get_all_jobs()
            if not jobs:
                print("No jobs yet.")
                return
            colors = {"queued": "#ffeeba", "running": "#b8daff",
                      "completed": "#c3e6cb", "failed": "#f5c6cb"}
            html  = ("<table style='width:100%;border-collapse:collapse;'>"
                     "<tr style='background:#333;color:white;'>"
                     "<th style='padding:6px'>ID</th>"
                     "<th style='padding:6px'>Name</th>"
                     "<th style='padding:6px'>State</th>"
                     "<th style='padding:6px'>Chunks</th>"
                     "<th style='padding:6px'>Nodes</th>"
                     "<th style='padding:6px'>Submitted</th>"
                     "</tr>")
            for j in jobs:
                bg    = colors.get(j["state"], "#fff")
                nodes = ", ".join(j["assigned_nodes"]) if j["assigned_nodes"] else "-"
                html += (f"<tr style='background:{bg}'>"
                         f"<td style='padding:6px'>{j['job_id']}</td>"
                         f"<td style='padding:6px'>{j['job_name']}</td>"
                         f"<td style='padding:6px'>{j['state']}</td>"
                         f"<td style='padding:6px'>{j['chunks']}</td>"
                         f"<td style='padding:6px'>{nodes}</td>"
                         f"<td style='padding:6px'>{j['submitted_at'][:19]}</td>"
                         "</tr>")
            html += "</table>"
            display(HTML(html))

    def _on_detail(self, _button):
        self.detail_out.clear_output()
        with self.detail_out:
            job_id = self.detail_id_input.value.strip()
            if not job_id:
                print("Enter a job ID.")
                return
            try:
                status = self.monitor.check_job_status(job_id)
                for k, v in status.items():
                    print(f"{k}: {v}")

                job_tasks = sorted(
                    [t for t in self.scheduler.completed_tasks if t.parent_job_id == job_id],
                    key=lambda t: t.chunk_index,
                )
                if job_tasks:
                    print("\n--- Subtask Output ---")
                    for task in job_tasks:
                        sid = f"{job_id}-{task.chunk_index}"
                        result = self.executor.results.get(sid)
                        print(f"\n[Chunk {task.chunk_index}/{task.total_chunks}]"
                              f" status={task.status}")
                        if result:
                            print(f"  duration={result.duration}s  rc={result.return_code}")
                            if result.stdout:
                                print(f"  stdout:\n    {result.stdout}")
                            if result.stderr:
                                print(f"  stderr:\n    {result.stderr}")
            except KeyError:
                print(f"Job '{job_id}' not found.")

    def _on_cancel(self, _button):
        self.cancel_out.clear_output()
        with self.cancel_out:
            job_id = self.cancel_id_input.value.strip()
            if not job_id:
                print("Enter a job ID.")
                return
            try:
                ok = self.job_manager.cancel_job(job_id)
                print(f"Job {job_id} {'cancelled' if ok else 'could not be cancelled'}.")
            except KeyError:
                print(f"Job '{job_id}' not found.")

    def _on_cluster_refresh(self, _button):
        self.cluster_out.clear_output()
        with self.cluster_out:
            status = self.cluster.get_cluster_status()
            if not status:
                print("No cluster initialized.")
                return

            # Count completed tasks per node from scheduler history
            completed_per_node = {}
            for t in self.scheduler.completed_tasks:
                completed_per_node[t.assigned_node] = \
                    completed_per_node.get(t.assigned_node, 0) + 1

            html  = ("<table style='width:100%;border-collapse:collapse;'>"
                     "<tr style='background:#333;color:white;'>"
                     "<th style='padding:6px'>Node</th>"
                     "<th style='padding:6px'>Cores</th>"
                     "<th style='padding:6px'>Used</th>"
                     "<th style='padding:6px'>RAM (MB)</th>"
                     "<th style='padding:6px'>RAM Used</th>"
                     "<th style='padding:6px'>Utilization</th>"
                     "<th style='padding:6px'>Active Tasks</th>"
                     "<th style='padding:6px'>Completed Tasks</th>"
                     "<th style='padding:6px'>Status</th>"
                     "</tr>")
            for name, info in status.items():
                util        = info["utilization"]
                node_status = info["status"]
                completed   = completed_per_node.get(name, 0)
                if node_status == "unresponsive":
                    bg          = "#f5c6cb"
                    status_html = "<b style='color:#c0392b'>unresponsive</b>"
                elif node_status == "busy":
                    bg          = "#b8daff"
                    status_html = "<b style='color:#1a6bbd'>busy</b>"
                else:
                    bg          = "#c3e6cb"
                    status_html = "<span style='color:#1e7e34'>idle</span>"
                html += (f"<tr style='background:{bg}'>"
                         f"<td style='padding:6px'>{name}</td>"
                         f"<td style='padding:6px'>{info['cores']}</td>"
                         f"<td style='padding:6px'>{info['cores_used']}</td>"
                         f"<td style='padding:6px'>{info['ram_mb']}</td>"
                         f"<td style='padding:6px'>{info['ram_used_mb']}</td>"
                         f"<td style='padding:6px'>{util}%</td>"
                         f"<td style='padding:6px'>{info['tasks']}</td>"
                         f"<td style='padding:6px'>{completed}</td>"
                         f"<td style='padding:6px'>{status_html}</td>"
                         "</tr>")
            html += "</table>"
            display(HTML(html))

    def _on_logs_refresh(self, _button):
        self.logs_out.clear_output()
        with self.logs_out:
            print("=== System Event Logs ===")
            logs = self.monitor.get_logs(last_n=40)
            if not logs:
                print("No monitor logs yet.")
            else:
                for entry in logs:
                    print(entry)

            print("\n=== Task Output ===")
            if not self.executor.results:
                print("No task results yet.")
            else:
                for sid, result in sorted(self.executor.results.items()):
                    status_str = "OK" if result.success else "FAILED"
                    print(f"\n[{sid}] {status_str}"
                          f" ({result.duration}s, rc={result.return_code})")
                    if result.stdout:
                        print(f"  stdout:\n    {result.stdout}")
                    if result.stderr:
                        print(f"  stderr:\n    {result.stderr}")