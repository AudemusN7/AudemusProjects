from __future__ import annotations

import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

from PySide6.QtCore import QSettings, QThread, Qt, Signal
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QPlainTextEdit,
    QSizePolicy,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


APP_NAME = "Nexus Upload Tool"
APP_VERSION = "1.0.0"
DEFAULT_API_BASE = "https://api.nexusmods.com/v3"
DEFAULT_POLL_SECONDS = 2.0
DEFAULT_POLL_ATTEMPTS = 60
DEFAULT_UPLOAD_WORKERS = 6


class NexusUploadError(RuntimeError):
    pass


@dataclass
class UploadOptions:
    api_key: str
    api_base: str
    file_path: Path
    file_group_id: str
    version: str
    display_name: str
    description: str
    file_category: str
    archive_existing_file: bool
    primary_mod_manager_download: bool | None
    allow_mod_manager_download: bool | None
    show_requirements_pop_up: bool | None


class NexusUploadClient:
    def __init__(self, api_key: str, api_base: str = DEFAULT_API_BASE):
        self.api_key = api_key.strip()
        self.api_base = api_base.strip().rstrip("/") or DEFAULT_API_BASE

    def upload(self, options: UploadOptions, progress_callback, log_callback) -> dict[str, Any]:
        file_path = options.file_path
        if not file_path.exists():
            raise NexusUploadError(f"File does not exist: {file_path}")
        if not file_path.is_file():
            raise NexusUploadError(f"Selected path is not a file: {file_path}")

        size_bytes = file_path.stat().st_size
        if size_bytes <= 0:
            raise NexusUploadError("Selected file is empty.")

        log_callback(f"Requesting multipart upload for {file_path.name} ({format_size(size_bytes)})")
        multipart = self._create_multipart_upload(file_path.name, size_bytes)

        data = require_dict(multipart, "data")
        upload_id = str(require_value(data, "id"))
        part_urls = require_list(data, "part_presigned_urls")
        part_size = int(require_value(data, "part_size_bytes"))
        complete_url = str(require_value(data, "complete_presigned_url"))

        if not part_urls:
            raise NexusUploadError("Nexus returned no upload part URLs.")
        if part_size <= 0:
            raise NexusUploadError(f"Nexus returned an invalid part size: {part_size}")

        log_callback(f"Created upload {upload_id}: {len(part_urls)} part(s), {format_size(part_size)} each")
        parts = self._upload_parts(file_path, part_urls, part_size, size_bytes, progress_callback, log_callback)

        log_callback("Completing multipart upload")
        self._complete_multipart_upload(complete_url, parts)

        log_callback("Finalising upload with Nexus Mods")
        finalised = self._finalise_upload(upload_id)
        finalised_data = finalised.get("data", {})
        if isinstance(finalised_data, dict):
            log_callback(f"Finalised upload {finalised_data.get('id', upload_id)}; state: {finalised_data.get('state', 'unknown')}")

        log_callback("Waiting for Nexus processing to finish")
        available = self._wait_until_available(upload_id, log_callback)

        log_callback("Attaching upload to file group/version")
        updated = self._update_file_group(upload_id, options)
        file_uid = updated.get("data", {}).get("id") if isinstance(updated.get("data"), dict) else None
        if file_uid:
            log_callback(f"File updated successfully. Nexus file UID: {file_uid}")
        else:
            log_callback("File updated successfully.")

        progress_callback(100, "Upload complete")
        return {
            "upload_id": upload_id,
            "available": available,
            "update": updated,
            "file_uid": file_uid,
        }

    def _create_multipart_upload(self, filename: str, size_bytes: int) -> dict[str, Any]:
        payload = {
            "filename": filename,
            "size_bytes": str(size_bytes),
        }
        return self._api_json("POST", "/uploads/multipart", payload)

    def _upload_parts(
        self,
        file_path: Path,
        part_urls: list[Any],
        part_size: int,
        size_bytes: int,
        progress_callback,
        log_callback,
    ) -> list[dict[str, Any]]:
        parts_by_number: dict[int, dict[str, Any]] = {}
        bytes_uploaded = 0
        progress_lock = Lock()
        worker_count = min(DEFAULT_UPLOAD_WORKERS, len(part_urls))

        log_callback(f"Uploading with {worker_count} parallel worker(s)")

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            for index, raw_url in enumerate(part_urls, start=1):
                offset = (index - 1) * part_size
                chunk_size = min(part_size, size_bytes - offset)
                if chunk_size <= 0:
                    raise NexusUploadError(f"Calculated invalid chunk size for part {index}: {chunk_size}")

                future = executor.submit(
                    self._upload_part_from_file,
                    file_path,
                    str(raw_url),
                    index,
                    len(part_urls),
                    offset,
                    chunk_size,
                    log_callback,
                )
                futures[future] = chunk_size

            for future in as_completed(futures):
                part = future.result()
                part_number = int(part["partNumber"])
                parts_by_number[part_number] = part

                with progress_lock:
                    bytes_uploaded += futures[future]
                    percent = min(99, int((bytes_uploaded / size_bytes) * 100))
                    progress_callback(percent, f"Uploaded {format_size(bytes_uploaded)} of {format_size(size_bytes)}")

        if len(parts_by_number) != len(part_urls):
            raise NexusUploadError(f"Uploaded {len(parts_by_number)} parts, but Nexus expected {len(part_urls)}.")

        return [parts_by_number[index] for index in range(1, len(part_urls) + 1)]

    def _upload_part_from_file(
        self,
        file_path: Path,
        url: str,
        index: int,
        total_parts: int,
        offset: int,
        chunk_size: int,
        log_callback,
    ) -> dict[str, Any]:
        log_callback(f"Uploading part {index}/{total_parts} ({format_size(chunk_size)})")
        with file_path.open("rb") as handle:
            handle.seek(offset)
            chunk = handle.read(chunk_size)

        if len(chunk) != chunk_size:
            raise NexusUploadError(
                f"Could not read full chunk for part {index}: expected {chunk_size} bytes, got {len(chunk)}."
            )

        etag = self._put_upload_part(url, chunk)
        return {"partNumber": index, "etag": etag.replace('"', "")}

    def _put_upload_part(self, url: str, chunk: bytes) -> str:
        request = urllib.request.Request(
            url,
            data=chunk,
            method="PUT",
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(chunk)),
            },
        )
        with self._open(request) as response:
            etag = response.headers.get("ETag")
            if not etag:
                raise NexusUploadError("Upload part succeeded, but no ETag was returned.")
            return etag

    def _complete_multipart_upload(self, complete_url: str, parts: list[dict[str, Any]]) -> None:
        body = self._multipart_complete_xml(parts).encode("utf-8")
        request = urllib.request.Request(
            complete_url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/xml",
                "Content-Length": str(len(body)),
            },
        )
        with self._open(request):
            return

    def _finalise_upload(self, upload_id: str) -> dict[str, Any]:
        return self._api_json("POST", f"/uploads/{upload_id}/finalise", None)

    def _wait_until_available(self, upload_id: str, log_callback) -> dict[str, Any]:
        delay = DEFAULT_POLL_SECONDS
        for attempt in range(1, DEFAULT_POLL_ATTEMPTS + 1):
            response = self._api_json("GET", f"/uploads/{upload_id}", None)
            data = response.get("data", {})
            state = data.get("state", "unknown") if isinstance(data, dict) else "unknown"
            log_callback(f"Processing state: {state} ({attempt}/{DEFAULT_POLL_ATTEMPTS})")
            if state == "available":
                return response

            time.sleep(delay)
            delay = min(delay * 1.5, 30.0)

        raise NexusUploadError(f"Upload processing timed out after {DEFAULT_POLL_ATTEMPTS} checks.")

    def _update_file_group(self, upload_id: str, options: UploadOptions) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "upload_id": upload_id,
            "name": options.display_name or options.file_path.name,
            "version": options.version,
            "file_category": options.file_category or "main",
            "archive_existing_file": options.archive_existing_file,
        }
        if options.description:
            payload["description"] = options.description
        if options.primary_mod_manager_download is not None:
            payload["primary_mod_manager_download"] = options.primary_mod_manager_download
        if options.allow_mod_manager_download is not None:
            payload["allow_mod_manager_download"] = options.allow_mod_manager_download
        if options.show_requirements_pop_up is not None:
            payload["show_requirements_pop_up"] = options.show_requirements_pop_up

        return self._api_json("POST", f"/mod-file-update-groups/{options.file_group_id}/versions", payload)

    def _api_json(self, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        url = f"{self.api_base}{path}"
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {
            "apikey": self.api_key,
            "User-Agent": f"{APP_NAME}/{APP_VERSION}",
            "Accept": "application/json",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))

        request = urllib.request.Request(url, data=body, method=method, headers=headers)
        with self._open(request) as response:
            raw = response.read().decode("utf-8", errors="replace")
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise NexusUploadError(f"Nexus returned non-JSON response from {path}: {raw[:500]}") from exc

    def _open(self, request: urllib.request.Request):
        try:
            return urllib.request.urlopen(request, timeout=None)
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise NexusUploadError(f"HTTP {exc.code} from {request.full_url}: {details}") from exc
        except urllib.error.URLError as exc:
            raise NexusUploadError(f"Network error while calling {request.full_url}: {exc.reason}") from exc

    @staticmethod
    def _multipart_complete_xml(parts: list[dict[str, Any]]) -> str:
        entries = []
        for part in parts:
            entries.append(
                "  <Part>\n"
                f"    <PartNumber>{part['partNumber']}</PartNumber>\n"
                f"    <ETag>{xml_escape(str(part['etag']))}</ETag>\n"
                "  </Part>"
            )
        return "<CompleteMultipartUpload>\n" + "\n".join(entries) + "\n</CompleteMultipartUpload>"


class UploadWorker(QThread):
    progress_changed = Signal(int, str)
    log_line = Signal(str)
    upload_finished = Signal(dict)
    upload_failed = Signal(str)

    def __init__(self, options: UploadOptions):
        super().__init__()
        self.options = options

    def run(self) -> None:
        try:
            client = NexusUploadClient(self.options.api_key, self.options.api_base)
            result = client.upload(self.options, self.progress_changed.emit, self.log_line.emit)
            self.upload_finished.emit(result)
        except Exception as exc:
            self.log_line.emit(traceback.format_exc())
            self.upload_failed.emit(str(exc))


class NexusUploaderWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings = QSettings("User", "NexusUploader")
        self.worker: UploadWorker | None = None

        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.resize(920, 720)

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        layout.addWidget(self._build_connection_group())
        layout.addWidget(self._build_file_group())
        layout.addWidget(self._build_metadata_group())
        layout.addWidget(self._build_options_group())
        layout.addLayout(self._build_actions())
        layout.addWidget(self._build_log_group(), stretch=1)

        self._load_settings()
        self._sync_display_name_from_file()

    def _build_connection_group(self) -> QGroupBox:
        group = QGroupBox("Connection")
        layout = QGridLayout(group)

        self.api_key_edit = QLineEdit()
        self.api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_edit.setPlaceholderText("Nexus Mods API key")

        self.show_key_checkbox = QCheckBox("Show")
        self.show_key_checkbox.toggled.connect(self._toggle_api_key_visibility)

        self.save_key_checkbox = QCheckBox("Save API key locally")
        self.save_key_checkbox.setToolTip("Stores the key in Qt app settings on this Windows profile.")

        self.api_base_edit = QLineEdit(DEFAULT_API_BASE)
        self.api_base_edit.setPlaceholderText(DEFAULT_API_BASE)

        layout.addWidget(QLabel("API key"), 0, 0)
        layout.addWidget(self.api_key_edit, 0, 1)
        layout.addWidget(self.show_key_checkbox, 0, 2)
        layout.addWidget(self.save_key_checkbox, 0, 3)
        layout.addWidget(QLabel("API base"), 1, 0)
        layout.addWidget(self.api_base_edit, 1, 1, 1, 3)

        return group

    def _build_file_group(self) -> QGroupBox:
        group = QGroupBox("Local File")
        layout = QGridLayout(group)

        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("Select the archive you want to upload")
        self.file_path_edit.textChanged.connect(self._sync_display_name_from_file)

        self.browse_button = QPushButton("Browse")
        self.browse_button.clicked.connect(self._browse_file)

        layout.addWidget(QLabel("Archive"), 0, 0)
        layout.addWidget(self.file_path_edit, 0, 1)
        layout.addWidget(self.browse_button, 0, 2)

        return group

    def _build_metadata_group(self) -> QGroupBox:
        group = QGroupBox("Nexus File Details")
        layout = QFormLayout(group)

        self.file_group_id_edit = QLineEdit()
        self.file_group_id_edit.setPlaceholderText("Existing Nexus file group ID")

        self.version_edit = QLineEdit()
        self.version_edit.setPlaceholderText("1.0.0")

        self.display_name_edit = QLineEdit()
        self.display_name_edit.setPlaceholderText("Defaults to selected archive name")

        self.category_combo = QComboBox()
        self.category_combo.setEditable(True)
        self.category_combo.addItems(["main", "optional", "miscellaneous", "old_version"])

        self.description_edit = QTextEdit()
        self.description_edit.setPlaceholderText("Optional file description")
        self.description_edit.setFixedHeight(90)

        layout.addRow("File group ID", self.file_group_id_edit)
        layout.addRow("Version", self.version_edit)
        layout.addRow("Display name", self.display_name_edit)
        layout.addRow("Category", self.category_combo)
        layout.addRow("Description", self.description_edit)

        return group

    def _build_options_group(self) -> QGroupBox:
        group = QGroupBox("Upload Options")
        layout = QGridLayout(group)

        self.archive_existing_checkbox = QCheckBox("Archive existing file")
        self.archive_existing_checkbox.setChecked(False)

        self.primary_mod_manager_checkbox = TriStateCheckBox("Primary mod manager download")
        self.allow_mod_manager_checkbox = TriStateCheckBox("Allow mod manager download")
        self.requirements_popup_checkbox = TriStateCheckBox("Show requirements pop-up")

        layout.addWidget(self.archive_existing_checkbox, 0, 0)
        layout.addWidget(self.primary_mod_manager_checkbox, 0, 1)
        layout.addWidget(self.allow_mod_manager_checkbox, 1, 0)
        layout.addWidget(self.requirements_popup_checkbox, 1, 1)

        return group

    def _build_actions(self) -> QHBoxLayout:
        layout = QHBoxLayout()

        self.upload_button = QPushButton("Upload")
        self.upload_button.clicked.connect(self._start_upload)

        self.save_profile_button = QPushButton("Save Profile")
        self.save_profile_button.clicked.connect(self._save_settings)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        self.status_label = QLabel("Ready")
        self.status_label.setMinimumWidth(180)

        layout.addWidget(self.upload_button)
        layout.addWidget(self.save_profile_button)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.status_label)

        return layout

    def _build_log_group(self) -> QGroupBox:
        group = QGroupBox("Log")
        layout = QVBoxLayout(group)
        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        self.log_edit.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        layout.addWidget(self.log_edit)
        return group

    def _browse_file(self) -> None:
        start_dir = self.settings.value("last_file_dir", str(Path.home()))
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select archive to upload",
            str(start_dir),
            "Archives (*.zip *.7z *.rar *.tar *.gz *.bz2);;All files (*.*)",
        )
        if file_path:
            self.file_path_edit.setText(file_path)
            self.settings.setValue("last_file_dir", str(Path(file_path).parent))

    def _start_upload(self) -> None:
        try:
            options = self._collect_options()
        except NexusUploadError as exc:
            QMessageBox.warning(self, "Missing details", str(exc))
            return

        self._save_settings()
        self._set_busy(True)
        self.progress_bar.setValue(0)
        self.log_edit.clear()
        self._append_log("Starting upload")

        self.worker = UploadWorker(options)
        self.worker.progress_changed.connect(self._update_progress)
        self.worker.log_line.connect(self._append_log)
        self.worker.upload_finished.connect(self._upload_finished)
        self.worker.upload_failed.connect(self._upload_failed)
        self.worker.start()

    def _collect_options(self) -> UploadOptions:
        api_key = self.api_key_edit.text().strip()
        file_path_text = self.file_path_edit.text().strip()
        file_group_id = self.file_group_id_edit.text().strip()
        version = self.version_edit.text().strip()
        category = self.category_combo.currentText().strip()

        if not api_key:
            raise NexusUploadError("Enter your Nexus Mods API key.")
        if not file_path_text:
            raise NexusUploadError("Select a local archive to upload.")
        if not file_group_id:
            raise NexusUploadError("Enter the Nexus file group ID.")
        if not version:
            raise NexusUploadError("Enter the version.")
        if not category:
            raise NexusUploadError("Enter a file category.")

        return UploadOptions(
            api_key=api_key,
            api_base=self.api_base_edit.text().strip() or DEFAULT_API_BASE,
            file_path=Path(file_path_text),
            file_group_id=file_group_id,
            version=version,
            display_name=self.display_name_edit.text().strip() or Path(file_path_text).name,
            description=self.description_edit.toPlainText().strip(),
            file_category=category,
            archive_existing_file=self.archive_existing_checkbox.isChecked(),
            primary_mod_manager_download=self.primary_mod_manager_checkbox.value(),
            allow_mod_manager_download=self.allow_mod_manager_checkbox.value(),
            show_requirements_pop_up=self.requirements_popup_checkbox.value(),
        )

    def _upload_finished(self, result: dict) -> None:
        self._set_busy(False)
        self.progress_bar.setValue(100)
        self.status_label.setText("Complete")
        file_uid = result.get("file_uid")
        message = "Upload complete."
        if file_uid:
            message += f"\n\nNexus file UID: {file_uid}"
        QMessageBox.information(self, "Upload complete", message)

    def _upload_failed(self, message: str) -> None:
        self._set_busy(False)
        self.status_label.setText("Failed")
        self._append_log(f"FAILED: {message}")
        QMessageBox.critical(self, "Upload failed", message)

    def _update_progress(self, value: int, text: str) -> None:
        self.progress_bar.setValue(value)
        self.status_label.setText(text)

    def _append_log(self, text: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        self.log_edit.appendPlainText(f"[{timestamp}] {text}")
        self.log_edit.verticalScrollBar().setValue(self.log_edit.verticalScrollBar().maximum())

    def _set_busy(self, busy: bool) -> None:
        self.upload_button.setEnabled(not busy)
        self.save_profile_button.setEnabled(not busy)
        self.browse_button.setEnabled(not busy)
        if busy:
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        else:
            while QApplication.overrideCursor() is not None:
                QApplication.restoreOverrideCursor()

    def _toggle_api_key_visibility(self, visible: bool) -> None:
        mode = QLineEdit.EchoMode.Normal if visible else QLineEdit.EchoMode.Password
        self.api_key_edit.setEchoMode(mode)

    def _sync_display_name_from_file(self) -> None:
        if self.display_name_edit.text().strip():
            return
        path_text = self.file_path_edit.text().strip()
        if path_text:
            self.display_name_edit.setPlaceholderText(Path(path_text).name)

    def _load_settings(self) -> None:
        self.api_base_edit.setText(str(self.settings.value("api_base", DEFAULT_API_BASE)))
        self.file_path_edit.setText(str(self.settings.value("file_path", "")))
        self.file_group_id_edit.setText(str(self.settings.value("file_group_id", "")))
        self.version_edit.setText(str(self.settings.value("version", "")))
        self.display_name_edit.setText(str(self.settings.value("display_name", "")))
        self.description_edit.setPlainText(str(self.settings.value("description", "")))
        self.archive_existing_checkbox.setChecked(read_bool(self.settings, "archive_existing_file", False))
        self.primary_mod_manager_checkbox.setValue(read_optional_bool(self.settings, "primary_mod_manager_download"))
        self.allow_mod_manager_checkbox.setValue(read_optional_bool(self.settings, "allow_mod_manager_download"))
        self.requirements_popup_checkbox.setValue(read_optional_bool(self.settings, "show_requirements_pop_up"))

        category = str(self.settings.value("file_category", "main"))
        index = self.category_combo.findText(category)
        if index >= 0:
            self.category_combo.setCurrentIndex(index)
        else:
            self.category_combo.setEditText(category)

        saved_key = str(self.settings.value("api_key", ""))
        if saved_key:
            self.api_key_edit.setText(saved_key)
            self.save_key_checkbox.setChecked(True)

    def _save_settings(self) -> None:
        self.settings.setValue("api_base", self.api_base_edit.text().strip() or DEFAULT_API_BASE)
        self.settings.setValue("file_path", self.file_path_edit.text().strip())
        self.settings.setValue("file_group_id", self.file_group_id_edit.text().strip())
        self.settings.setValue("version", self.version_edit.text().strip())
        self.settings.setValue("display_name", self.display_name_edit.text().strip())
        self.settings.setValue("description", self.description_edit.toPlainText().strip())
        self.settings.setValue("file_category", self.category_combo.currentText().strip())
        self.settings.setValue("archive_existing_file", self.archive_existing_checkbox.isChecked())
        self.settings.setValue("primary_mod_manager_download", self.primary_mod_manager_checkbox.value())
        self.settings.setValue("allow_mod_manager_download", self.allow_mod_manager_checkbox.value())
        self.settings.setValue("show_requirements_pop_up", self.requirements_popup_checkbox.value())

        if self.save_key_checkbox.isChecked():
            self.settings.setValue("api_key", self.api_key_edit.text().strip())
        else:
            self.settings.remove("api_key")

        self._append_log("Profile saved")


class TriStateCheckBox(QCheckBox):
    def __init__(self, text: str):
        super().__init__(text)
        self.setTristate(True)
        self.setCheckState(Qt.CheckState.PartiallyChecked)
        self.setToolTip("Partially checked means leave this option unset.")

    def value(self) -> bool | None:
        state = self.checkState()
        if state == Qt.CheckState.PartiallyChecked:
            return None
        return state == Qt.CheckState.Checked

    def setValue(self, value: bool | None) -> None:
        if value is None:
            self.setCheckState(Qt.CheckState.PartiallyChecked)
        elif value:
            self.setCheckState(Qt.CheckState.Checked)
        else:
            self.setCheckState(Qt.CheckState.Unchecked)


def read_bool(settings: QSettings, key: str, default: bool) -> bool:
    value = settings.value(key, default)
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes"}


def read_optional_bool(settings: QSettings, key: str) -> bool | None:
    value = settings.value(key, None)
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    lowered = str(value).lower()
    if lowered in {"none", "null"}:
        return None
    return lowered in {"1", "true", "yes"}


def require_dict(source: dict[str, Any], key: str) -> dict[str, Any]:
    value = source.get(key)
    if not isinstance(value, dict):
        raise NexusUploadError(f"Nexus response is missing object field: {key}")
    return value


def require_list(source: dict[str, Any], key: str) -> list[Any]:
    value = source.get(key)
    if not isinstance(value, list):
        raise NexusUploadError(f"Nexus response is missing list field: {key}")
    return value


def require_value(source: dict[str, Any], key: str) -> Any:
    value = source.get(key)
    if value in (None, ""):
        raise NexusUploadError(f"Nexus response is missing field: {key}")
    return value


def xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def format_size(size_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size_bytes} B"


def main() -> int:
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)

    window = NexusUploaderWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
