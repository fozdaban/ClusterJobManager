"""
Interface Module
Provides a Jupyter notebook widget interface, and file-based job submission.
"""

import ipywidgets as widgets
from IPython.display import display, clear_output
from jobsubmission import JobSubmissionManager, JobDescription


class NotebookUI:
    """Interactive Jupyter notebook interface using ipywidgets."""

    def __init__(self, manager: JobSubmissionManager = None):
        self.manager = manager or JobSubmissionManager()
        self.output = widgets.Output()

    # ---- Widget Builders ----

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
            description="Time Limit:", value=300, min=0
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
            self.output,
        ])
        return form

    def build_file_upload_widget(self):
        self.file_upload = widgets.FileUpload(
            accept=".yaml,.yml,.json", multiple=False, description="Upload Job File"
        )
        self.upload_button = widgets.Button(
            description="Submit File", button_style="info", icon="upload"
        )
        self.upload_button.on_click(self._on_file_upload_click)

        upload_box = widgets.VBox([
            widgets.HTML("<h3>Upload Job File</h3>"),
            self.file_upload,
            self.upload_button,
            self.output,
        ])
        return upload_box

    def build_status_dashboard(self):
        self.refresh_button = widgets.Button(
            description="Refresh", button_style="primary", icon="refresh"
        )
        self.refresh_button.on_click(self._on_refresh_click)

        self.status_output = widgets.Output()

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
        self.detail_button.on_click(self._on_detail_click)

        self.detail_output = widgets.Output()

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
        self.cancel_button.on_click(self._on_cancel_click)

        self.cancel_output = widgets.Output()

        cancel_view = widgets.VBox([
            widgets.HTML("<h3>Cancel a Job</h3>"),
            widgets.HBox([self.cancel_id_input, self.cancel_button]),
            self.cancel_output,
        ])
        return cancel_view

    def display_all(self):
        tabs = widgets.Tab()
        tabs.children = [
            self.build_submission_form(),
            self.build_file_upload_widget(),
            self.build_status_dashboard(),
            self.build_job_detail_view(),
            self.build_cancel_widget(),
        ]
        tabs.set_title(0, "Submit Job")
        tabs.set_title(1, "Upload File")
        tabs.set_title(2, "Dashboard")
        tabs.set_title(3, "Job Details")
        tabs.set_title(4, "Cancel Job")

        display(tabs)

    # Event Handlers

    def _on_submit_click(self, button):
        """Handle submit button click from the form."""
        pass

    def _on_file_upload_click(self, button):
        """Handle file upload submission."""
        pass

    def _on_refresh_click(self, button):
        """Handle refresh button click on the dashboard."""
        pass

    def _on_detail_click(self, button):
        """Handle job detail lookup."""
        pass

    def _on_cancel_click(self, button):
        """Handle job cancel button click."""
        pass

