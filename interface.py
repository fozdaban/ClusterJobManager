"""
User Interface Module
Jupyter notebook widget interface for the cluster job manager.
"""

import ipywidgets as widgets
from IPython.display import display, clear_output, HTML
from jobsubmission import JobSubmissionManager
from scheduler import ClusterManager, Scheduler
from executor import Executor
from monitor import Monitor
from persistence import Persistence


class NotebookUI:

    def __init__(self, job_manager=None, cluster=None, scheduler=None,
                 executor=None, monitor=None, persistence=None):
        self.job_manager = job_manager or JobSubmissionManager()
        self.cluster = cluster or ClusterManager()
        self.scheduler = scheduler or Scheduler(self.cluster, self.job_manager)
        self.executor = executor or Executor(self.cluster)
        self.monitor = monitor or Monitor(self.scheduler, self.cluster, self.job_manager)
        self.persistence = persistence or Persistence()
        self.output = widgets.Output()

    #   Widget Builders  

    def build_submission_form(self):
        self.job_name_input = widgets.Text(
            description="Job Name:", placeholder="e.g. matrix_multiply"
        )
        self.command_input = widgets.Text(
            description="Command:", placeholder="e.g. python3 script.py"
        )
        self.cpus_input = widgets.IntSlider(
            description="CPUs:", min=1, max=16, value=1
        )
        self.memory_input = widgets.IntSlider(
            description="Memory (MB):", min=512, max=16384, value=512, step=512
        )
        self.time_limit_input = widgets.IntText(
            description="Time Limit:", value=300
        )
        self.distributable_toggle = widgets.Checkbox(
            description="Distributable", value=False
        )
        self.chunks_input = widgets.IntSlider(
            description="Chunks:", min=1, max=10, value=1
        )
        self.submit_button = widgets.Button(
            description="Submit Job", button_style="success", icon="check"
        )
        self.submit_output = widgets.Output()
        self.submit_button.on_click(self._on_submit_click)

        form = widgets.VBox([
            widgets.HTML("<h3>Submit a Job</h3>"),
            self.job_name_input,
            self.command_input,
            self.cpus_input,
            self.memory_input,
            self.time_limit_input,
            self.distributable_toggle,
            self.chunks_input,
            self.submit_button,
            self.submit_output,
        ])
        return form

    def build_file_upload_widget(self):
        self.file_upload = widgets.FileUpload(
            accept=".yaml,.yml,.json", multiple=False, description="Upload Job File"
        )
        self.upload_button = widgets.Button(
            description="Submit File", button_style="info", icon="upload"
        )
        self.upload_output = widgets.Output()
        self.upload_button.on_click(self._on_file_upload_click)

        upload_box = widgets.VBox([
            widgets.HTML("<h3>Upload Job File</h3>"),
            self.file_upload,
            self.upload_button,
            self.upload_output,
        ])
        return upload_box

    def build_status_dashboard(self):
        self.refresh_button = widgets.Button(
            description="Refresh", button_style="primary", icon="refresh"
        )
        self.status_output = widgets.Output()
        self.refresh_button.on_click(self._on_refresh_click)

        dashboard = widgets.VBox([
            widgets.HTML("<h3>Job Status Dashboard</h3>"),
            self.refresh_button,
            self.status_output,
        ])
        return dashboard

    def build_job_detail_view(self):
        self.job_id_input = widgets.Text(
            description="Job ID:", placeholder="e.g. 1b292728"
        )
        self.detail_button = widgets.Button(
            description="Get Details", button_style="warning", icon="search"
        )
        self.detail_output = widgets.Output()
        self.detail_button.on_click(self._on_detail_click)

        detail_view = widgets.VBox([
            widgets.HTML("<h3>Job Details</h3>"),
            widgets.HBox([self.job_id_input, self.detail_button]),
            self.detail_output,
        ])
        return detail_view

    def build_cancel_widget(self):
        self.cancel_id_input = widgets.Text(
            description="Job ID:", placeholder="e.g. 1b292728"
        )
        self.cancel_button = widgets.Button(
            description="Cancel Job", button_style="danger", icon="times"
        )
        self.cancel_output = widgets.Output()
        self.cancel_button.on_click(self._on_cancel_click)

        cancel_view = widgets.VBox([
            widgets.HTML("<h3>Cancel a Job</h3>"),
            widgets.HBox([self.cancel_id_input, self.cancel_button]),
            self.cancel_output,
        ])
        return cancel_view

    def build_cluster_view(self):
        self.cluster_refresh = widgets.Button(
            description="Refresh", button_style="primary", icon="refresh"
        )
        self.cluster_output = widgets.Output()
        self.cluster_refresh.on_click(self._on_cluster_refresh)

        cluster_view = widgets.VBox([
            widgets.HTML("<h3>Cluster Status</h3>"),
            self.cluster_refresh,
            self.cluster_output,
        ])
        return cluster_view

    def display_all(self):
        tabs = widgets.Tab()
        tabs.children = [
            self.build_submission_form(),
            self.build_file_upload_widget(),
            self.build_status_dashboard(),
            self.build_job_detail_view(),
            self.build_cancel_widget(),
            self.build_cluster_view(),
        ]
        tabs.set_title(0, "Submit Job")
        tabs.set_title(1, "Upload File")
        tabs.set_title(2, "Dashboard")
        tabs.set_title(3, "Job Details")
        tabs.set_title(4, "Cancel Job")
        tabs.set_title(5, "Cluster")
        display(tabs)

    #   Event Handlers  

    def _on_submit_click(self, button):
        self.submit_output.clear_output()
        with self.submit_output:
            try:
                job_data = {
                    "job_name": self.job_name_input.value,
                    "command": self.command_input.value,
                    "resources": {
                        "cpus": self.cpus_input.value,
                        "memory_mb": self.memory_input.value,
                    },
                    "time_limit": self.time_limit_input.value,
                    "distributable": self.distributable_toggle.value,
                    "chunks": self.chunks_input.value,
                }
                job = self.job_manager.submit(job_data)
                success, msg = self.scheduler.schedule_job(job)

                if success:
                    active = [t for t in self.scheduler.active_tasks
                              if t.parent_job_id == job.job_id]
                    results = self.executor.run_job_subtasks(active, timeout=job.time_limit)
                    for t in active:
                        self.scheduler.release_resources(t)
                    self.scheduler.handle_job_completion(job.job_id)

                print(f"\nJob {job.job_id} — {job.state}")
                if job.result:
                    print(f"Output: {job.result}")
                if job.error:
                    print(f"Error: {job.error}")

            except ValueError as e:
                print(f"Validation error:\n{e}")

    def _on_file_upload_click(self, button):
        self.upload_output.clear_output()
        with self.upload_output:
            try:
                uploaded = self.file_upload.value
                if not uploaded:
                    print("No file selected")
                    return

                file_info = list(uploaded.values())[0] if isinstance(uploaded, dict) else uploaded[0]
                content = file_info["content"] if isinstance(file_info, dict) else file_info.content
                name = file_info["metadata"]["name"] if isinstance(file_info, dict) else file_info.name

                import yaml, json
                text = content.decode("utf-8") if isinstance(content, bytes) else content

                if name.endswith((".yaml", ".yml")):
                    job_data = yaml.safe_load(text)
                else:
                    job_data = json.loads(text)

                job = self.job_manager.submit(job_data)
                success, msg = self.scheduler.schedule_job(job)

                if success:
                    active = [t for t in self.scheduler.active_tasks
                              if t.parent_job_id == job.job_id]
                    results = self.executor.run_job_subtasks(active, timeout=job.time_limit)
                    for t in active:
                        self.scheduler.release_resources(t)
                    self.scheduler.handle_job_completion(job.job_id)

                print(f"\nJob {job.job_id} — {job.state}")
                if job.result:
                    print(f"Output: {job.result}")

            except Exception as e:
                print(f"Error: {e}")

    def _on_refresh_click(self, button):
        self.status_output.clear_output()
        with self.status_output:
            jobs = self.job_manager.get_all_jobs()
            if not jobs:
                print("No jobs submitted yet.")
                return

            html = "<table style='width:100%; border-collapse:collapse;'>"
            html += "<tr style='background:#333; color:white;'>"
            html += "<th style='padding:6px;'>ID</th>"
            html += "<th style='padding:6px;'>Name</th>"
            html += "<th style='padding:6px;'>State</th>"
            html += "<th style='padding:6px;'>Chunks</th>"
            html += "<th style='padding:6px;'>Nodes</th>"
            html += "<th style='padding:6px;'>Submitted</th>"
            html += "</tr>"

            colors = {
                "queued": "#ffeeba",
                "running": "#b8daff",
                "completed": "#c3e6cb",
                "failed": "#f5c6cb",
            }

            for j in jobs:
                bg = colors.get(j["state"], "#ffffff")
                html += f"<tr style='background:{bg};'>"
                html += f"<td style='padding:6px;'>{j['job_id']}</td>"
                html += f"<td style='padding:6px;'>{j['job_name']}</td>"
                html += f"<td style='padding:6px;'>{j['state']}</td>"
                html += f"<td style='padding:6px;'>{j['chunks']}</td>"
                html += f"<td style='padding:6px;'>{', '.join(j['assigned_nodes']) if j['assigned_nodes'] else '-'}</td>"
                html += f"<td style='padding:6px;'>{j['submitted_at'][:19]}</td>"
                html += "</tr>"

            html += "</table>"
            display(HTML(html))

    def _on_detail_click(self, button):
        self.detail_output.clear_output()
        with self.detail_output:
            job_id = self.job_id_input.value.strip()
            if not job_id:
                print("Enter a job ID")
                return
            try:
                status = self.monitor.check_job_status(job_id)
                for k, v in status.items():
                    print(f"{k}: {v}")

                job = self.job_manager.get_job(job_id)
                if job.result:
                    print(f"result: {job.result}")
                if job.error:
                    print(f"error: {job.error}")
            except KeyError:
                print(f"Job {job_id} not found")

    def _on_cancel_click(self, button):
        self.cancel_output.clear_output()
        with self.cancel_output:
            job_id = self.cancel_id_input.value.strip()
            if not job_id:
                print("Enter a job ID")
                return
            try:
                success = self.job_manager.cancel_job(job_id)
                if success:
                    print(f"Job {job_id} cancelled")
                else:
                    job = self.job_manager.get_job(job_id)
                    print(f"Cannot cancel — job is '{job.state}'")
            except KeyError:
                print(f"Job {job_id} not found")

    def _on_cluster_refresh(self, button):
        self.cluster_output.clear_output()
        with self.cluster_output:
            status = self.cluster.get_cluster_status()
            if not status:
                print("No cluster initialized")
                return

            html = "<table style='width:100%; border-collapse:collapse;'>"
            html += "<tr style='background:#333; color:white;'>"
            html += "<th style='padding:6px;'>Node</th>"
            html += "<th style='padding:6px;'>Cores</th>"
            html += "<th style='padding:6px;'>Used</th>"
            html += "<th style='padding:6px;'>RAM (MB)</th>"
            html += "<th style='padding:6px;'>RAM Used</th>"
            html += "<th style='padding:6px;'>Utilization</th>"
            html += "<th style='padding:6px;'>Tasks</th>"
            html += "<th style='padding:6px;'>Status</th>"
            html += "</tr>"

            for name, info in status.items():
                util = info["utilization"]
                bg = "#c3e6cb" if util < 50 else "#ffeeba" if util < 80 else "#f5c6cb"
                html += f"<tr style='background:{bg};'>"
                html += f"<td style='padding:6px;'>{name}</td>"
                html += f"<td style='padding:6px;'>{info['cores']}</td>"
                html += f"<td style='padding:6px;'>{info['cores_used']}</td>"
                html += f"<td style='padding:6px;'>{info['ram_mb']}</td>"
                html += f"<td style='padding:6px;'>{info['ram_used_mb']}</td>"
                html += f"<td style='padding:6px;'>{util}%</td>"
                html += f"<td style='padding:6px;'>{info['tasks']}</td>"
                html += f"<td style='padding:6px;'>{info['status']}</td>"
                html += "</tr>"

            html += "</table>"
            display(HTML(html))