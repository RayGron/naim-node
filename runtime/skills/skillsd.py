#!/usr/bin/env python3
import json
import os
import random
import signal
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


PLANE_NAME = os.environ.get("COMET_PLANE_NAME", "unknown")
INSTANCE_NAME = os.environ.get("COMET_INSTANCE_NAME", "skills-unknown")
INSTANCE_ROLE = os.environ.get("COMET_INSTANCE_ROLE", "skills")
NODE_NAME = os.environ.get("COMET_NODE_NAME", "unknown")
CONTROL_ROOT = os.environ.get("COMET_CONTROL_ROOT", "")
CONTROLLER_URL = os.environ.get("COMET_CONTROLLER_URL", "http://controller.internal:18080")
DB_PATH = Path(os.environ.get("COMET_SKILLS_DB_PATH", "/comet/private/skills.sqlite"))
STATUS_PATH = Path(
    os.environ.get("COMET_SKILLS_RUNTIME_STATUS_PATH", "/comet/private/skills-runtime-status.json")
)
PORT = int(os.environ.get("COMET_SKILLS_PORT", "18120"))
READY_PATH = Path("/tmp/comet-ready")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def uuid7_like() -> str:
    ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = random.getrandbits(12)
    rand_b = random.getrandbits(62)
    value = 0
    value |= ts_ms << 80
    value |= 0x7 << 76
    value |= rand_a << 64
    value |= 0b10 << 62
    value |= rand_b
    return str(uuid.UUID(int=value))


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def normalize_string_list(values, field_name: str, allow_empty: bool = False):
    if values is None:
        return []
    if not isinstance(values, list):
        raise ApiError(400, "invalid_request", f"{field_name} must be an array")
    result = []
    seen = set()
    for item in values:
        if not isinstance(item, str):
            raise ApiError(400, "invalid_request", f"{field_name} items must be strings")
        normalized = item.strip()
        if not normalized:
            if allow_empty:
                continue
            raise ApiError(400, "invalid_request", f"{field_name} items must not be empty")
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def normalize_skill_payload(payload, *, partial: bool):
    if not isinstance(payload, dict):
        raise ApiError(400, "invalid_request", "request body must be a JSON object")

    normalized = {}
    required = [] if partial else ["name", "description", "content"]
    for key in required:
        if key not in payload:
            raise ApiError(400, "invalid_request", f"{key} is required")

    for key in ("name", "description", "content"):
        if key in payload:
            value = payload.get(key)
            if not isinstance(value, str) or not value.strip():
                raise ApiError(400, "invalid_request", f"{key} must be a non-empty string")
            normalized[key] = value.strip()

    if "enabled" in payload:
        if not isinstance(payload["enabled"], bool):
            raise ApiError(400, "invalid_request", "enabled must be a boolean")
        normalized["enabled"] = payload["enabled"]
    elif not partial:
        normalized["enabled"] = True

    if "session_ids" in payload:
        normalized["session_ids"] = normalize_string_list(payload["session_ids"], "session_ids")
    elif not partial:
        normalized["session_ids"] = []

    if "comet_links" in payload:
        normalized["comet_links"] = normalize_string_list(payload["comet_links"], "comet_links")
    elif not partial:
        normalized["comet_links"] = []

    return normalized


class SkillsStore:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._init_schema()

    def _init_schema(self):
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS skills (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  description TEXT NOT NULL,
                  content TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS skill_session_bindings (
                  skill_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  PRIMARY KEY (skill_id, session_id),
                  FOREIGN KEY (skill_id) REFERENCES skills(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS skill_links (
                  skill_id TEXT NOT NULL,
                  link TEXT NOT NULL,
                  PRIMARY KEY (skill_id, link),
                  FOREIGN KEY (skill_id) REFERENCES skills(id) ON DELETE CASCADE
                );
                """
            )

    def _skill_from_row(self, row):
        skill_id = row["id"]
        session_ids = [
            item["session_id"]
            for item in self._conn.execute(
                "SELECT session_id FROM skill_session_bindings WHERE skill_id = ? ORDER BY session_id",
                (skill_id,),
            ).fetchall()
        ]
        comet_links = [
            item["link"]
            for item in self._conn.execute(
                "SELECT link FROM skill_links WHERE skill_id = ? ORDER BY link",
                (skill_id,),
            ).fetchall()
        ]
        return {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "content": row["content"],
            "enabled": bool(row["enabled"]),
            "session_ids": session_ids,
            "comet_links": comet_links,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_skills(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM skills ORDER BY updated_at DESC, name ASC"
            ).fetchall()
            return [self._skill_from_row(row) for row in rows]

    def get_skill(self, skill_id: str):
        with self._lock:
            row = self._conn.execute("SELECT * FROM skills WHERE id = ?", (skill_id,)).fetchone()
            if row is None:
                raise ApiError(404, "skill_not_found", f"skill '{skill_id}' not found")
            return self._skill_from_row(row)

    def create_skill(self, payload):
        skill = normalize_skill_payload(payload, partial=False)
        skill_id = uuid7_like()
        now = utc_now()
        with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO skills(id, name, description, content, enabled, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        skill_id,
                        skill["name"],
                        skill["description"],
                        skill["content"],
                        1 if skill["enabled"] else 0,
                        now,
                        now,
                    ),
                )
                self._replace_arrays_locked(skill_id, skill["session_ids"], skill["comet_links"])
            return self.get_skill(skill_id)

    def replace_skill(self, skill_id: str, payload, *, partial: bool):
        update = normalize_skill_payload(payload, partial=partial)
        with self._lock:
            current = self.get_skill(skill_id)
            merged = dict(current)
            merged.update(update)
            merged["session_ids"] = update.get("session_ids", current["session_ids"])
            merged["comet_links"] = update.get("comet_links", current["comet_links"])
            merged["updated_at"] = utc_now()
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE skills
                    SET name = ?, description = ?, content = ?, enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        merged["name"],
                        merged["description"],
                        merged["content"],
                        1 if merged["enabled"] else 0,
                        merged["updated_at"],
                        skill_id,
                    ),
                )
                self._replace_arrays_locked(
                    skill_id, merged["session_ids"], merged["comet_links"]
                )
            return self.get_skill(skill_id)

    def _replace_arrays_locked(self, skill_id: str, session_ids, comet_links):
        self._conn.execute("DELETE FROM skill_session_bindings WHERE skill_id = ?", (skill_id,))
        self._conn.execute("DELETE FROM skill_links WHERE skill_id = ?", (skill_id,))
        if session_ids:
            self._conn.executemany(
                "INSERT INTO skill_session_bindings(skill_id, session_id) VALUES (?, ?)",
                [(skill_id, session_id) for session_id in session_ids],
            )
        if comet_links:
            self._conn.executemany(
                "INSERT INTO skill_links(skill_id, link) VALUES (?, ?)",
                [(skill_id, link) for link in comet_links],
            )

    def delete_skill(self, skill_id: str):
        with self._lock:
            self.get_skill(skill_id)
            with self._conn:
                self._conn.execute("DELETE FROM skills WHERE id = ?", (skill_id,))

    def resolve_skills(self, payload):
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        session_id = payload.get("session_id")
        if session_id is not None:
            if not isinstance(session_id, str) or not session_id.strip():
                raise ApiError(400, "invalid_request", "session_id must be a non-empty string")
            session_id = session_id.strip()
        skill_ids = normalize_string_list(payload.get("skill_ids", []), "skill_ids")

        with self._lock:
            explicit = []
            seen = set()
            for skill_id in skill_ids:
                row = self._conn.execute(
                    "SELECT * FROM skills WHERE id = ? AND enabled = 1", (skill_id,)
                ).fetchone()
                if row is None:
                    raise ApiError(
                        400, "invalid_skill_reference", f"skill '{skill_id}' does not exist or is disabled"
                    )
                skill = self._skill_from_row(row)
                skill["source"] = "explicit"
                explicit.append(skill)
                seen.add(skill_id)

            session_bound = []
            if session_id:
                rows = self._conn.execute(
                    """
                    SELECT s.*
                    FROM skills s
                    JOIN skill_session_bindings ss ON ss.skill_id = s.id
                    WHERE ss.session_id = ? AND s.enabled = 1
                    ORDER BY s.updated_at DESC, s.name ASC
                    """,
                    (session_id,),
                ).fetchall()
                for row in rows:
                    if row["id"] in seen:
                        continue
                    skill = self._skill_from_row(row)
                    skill["source"] = "session"
                    session_bound.append(skill)
                    seen.add(row["id"])

        return {
            "skills": explicit + session_bound,
            "skills_session_id": session_id,
        }


STORE = SkillsStore(DB_PATH)


def set_ready_file(ready: bool):
    if ready:
        READY_PATH.parent.mkdir(parents=True, exist_ok=True)
        READY_PATH.write_text("ready\n", encoding="utf-8")
    elif READY_PATH.exists():
        READY_PATH.unlink()


def write_runtime_status(phase: str, ready: bool):
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    status = {
        "plane_name": PLANE_NAME,
        "control_root": CONTROL_ROOT,
        "controller_url": CONTROLLER_URL,
        "instance_name": INSTANCE_NAME,
        "instance_role": INSTANCE_ROLE,
        "node_name": NODE_NAME,
        "runtime_backend": "sqlite-http",
        "runtime_phase": phase,
        "started_at": utc_now(),
        "last_activity_at": utc_now(),
        "ready": ready,
        "active_model_ready": True,
        "gateway_plan_ready": False,
        "inference_ready": ready,
        "gateway_ready": False,
        "launch_ready": ready,
    }
    STATUS_PATH.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")


class SkillsHandler(BaseHTTPRequestHandler):
    server_version = "comet-skillsd/0.1"

    def log_message(self, format, *args):
        print("[comet-skills]", format % args, flush=True)

    def _read_json(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
          return {}
        raw = self.rfile.read(content_length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiError(400, "invalid_json", f"invalid JSON body: {exc.msg}") from exc

    def _send_json(self, status: int, payload):
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_error(self, exc: ApiError):
        self._send_json(exc.status, {"error": exc.code, "message": exc.message})

    def _split_path(self):
        path = urlparse(self.path).path
        return [segment for segment in path.split("/") if segment]

    def do_GET(self):
        try:
            parts = self._split_path()
            if parts == ["health"]:
                self._send_json(200, {"ok": True, "ready": True})
                return
            if parts == ["v1", "skills"]:
                self._send_json(200, {"skills": STORE.list_skills()})
                return
            if len(parts) == 3 and parts[0] == "v1" and parts[1] == "skills":
                self._send_json(200, STORE.get_skill(parts[2]))
                return
            raise ApiError(404, "not_found", "route not found")
        except ApiError as exc:
            self._handle_error(exc)

    def do_POST(self):
        try:
            parts = self._split_path()
            payload = self._read_json()
            if parts == ["v1", "skills"]:
                skill = STORE.create_skill(payload)
                self._send_json(201, skill)
                return
            if parts == ["v1", "skills", "resolve"]:
                self._send_json(200, STORE.resolve_skills(payload))
                return
            raise ApiError(404, "not_found", "route not found")
        except ApiError as exc:
            self._handle_error(exc)

    def do_PUT(self):
        try:
            parts = self._split_path()
            if len(parts) != 3 or parts[0] != "v1" or parts[1] != "skills":
                raise ApiError(404, "not_found", "route not found")
            self._send_json(200, STORE.replace_skill(parts[2], self._read_json(), partial=False))
        except ApiError as exc:
            self._handle_error(exc)

    def do_PATCH(self):
        try:
            parts = self._split_path()
            if len(parts) != 3 or parts[0] != "v1" or parts[1] != "skills":
                raise ApiError(404, "not_found", "route not found")
            self._send_json(200, STORE.replace_skill(parts[2], self._read_json(), partial=True))
        except ApiError as exc:
            self._handle_error(exc)

    def do_DELETE(self):
        try:
            parts = self._split_path()
            if len(parts) != 3 or parts[0] != "v1" or parts[1] != "skills":
                raise ApiError(404, "not_found", "route not found")
            STORE.delete_skill(parts[2])
            self._send_json(200, {"deleted": True, "id": parts[2]})
        except ApiError as exc:
            self._handle_error(exc)


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_runtime_status("starting", False)
    set_ready_file(False)

    server = ThreadingHTTPServer(("0.0.0.0", PORT), SkillsHandler)

    def shutdown_handler(signum, _frame):
        write_runtime_status("stopping", False)
        set_ready_file(False)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    write_runtime_status("running", True)
    set_ready_file(True)
    try:
        server.serve_forever()
    finally:
        write_runtime_status("stopped", False)
        set_ready_file(False)
        server.server_close()


if __name__ == "__main__":
    main()
