#!/usr/bin/env python3
"""
Simple local web dashboard for TTL (TikTok Live) capture data.
Reads log.csv and displays items with cost tracking and profit calculation.
"""

import csv
import json
import os
import platform
import re
import shutil
import smtplib
import subprocess
import sys
import time
import threading
import zipfile
import calendar
import secrets
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import psutil
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote, unquote
from typing import Optional
from flask import Flask, render_template, jsonify, request, send_file, Response, session, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash
try:
    from authlib.integrations.flask_client import OAuth
    HAS_OAUTH = True
except ImportError:
    HAS_OAUTH = False
import sqlite3

import db as db_module
from db import get_db, fetch_one, fetch_all, execute, execute_returning_id, is_postgres, translate
import show_utils
from show_utils import safe_filename, show_dir_path

app = Flask(__name__)
app.secret_key = os.environ.get(
    "FLASK_SECRET_KEY",
    os.environ.get("SECRET_KEY", "dev-secret-change-me"),
)

app.config["GOOGLE_CLIENT_ID"] = os.environ.get("GOOGLE_CLIENT_ID", "")
app.config["GOOGLE_CLIENT_SECRET"] = os.environ.get("GOOGLE_CLIENT_SECRET", "")

if HAS_OAUTH:
    oauth = OAuth(app)
else:
    oauth = None

if oauth and app.config["GOOGLE_CLIENT_ID"] and app.config["GOOGLE_CLIENT_SECRET"]:
    oauth.register(
        name="google",
        client_id=app.config["GOOGLE_CLIENT_ID"],
        client_secret=app.config["GOOGLE_CLIENT_SECRET"],
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )


@app.before_request
def require_auth_for_api():
    if request.path.startswith("/api/"):
        if request.path in {"/api/recording-status", "/api/extension-capture",
                             "/api/extension-sold",
                             "/api/extension-start", "/api/extension-stop",
                             "/api/shows",
                             # Debug + error ingest endpoints must work without
                             # login so they're usable during live Render
                             # incidents (when auth itself may be the thing
                             # that's broken). They never return row data.
                             "/api/debug/health",
                             "/api/debug/db-size",
                             "/api/debug/extension-errors",
                             "/api/extension-error"}:
            return None
        if not get_current_user():
            return jsonify({"error": "Unauthorized"}), 401
    return None


@app.after_request
def add_no_cache_headers(response):
    # Prevent Chrome from caching the dashboard UI during rapid updates.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    # CORS for Chrome extension
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-Key"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


def validate_api_key():
    """Validate the X-API-Key header. Returns org_id if valid, None if invalid."""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return None
    row = fetch_one("SELECT id FROM orgs WHERE api_key = ?", (api_key,))
    return row[0] if row else None


def get_org_from_api_key_or_session():
    """Get org_id from API key header first, then fall back to session."""
    org_id = validate_api_key()
    if org_id:
        return org_id
    return get_current_org_id()


# Paths
def get_captures_dir() -> Path:
    env_value = os.environ.get("TT_CAPTURES_DIR")
    if env_value:
        return Path(os.path.expanduser(env_value)).resolve()
    return (Path.home() / "Downloads" / "TT recorder live" / "captures").resolve()


CAPTURES_DIR = get_captures_dir()
CAPTURES_DIR.mkdir(parents=True, exist_ok=True)
CSV_FILE = CAPTURES_DIR / "log.csv"
DB_FILE = CAPTURES_DIR / "dashboard.db"
_UNSET = object()
BASE_DIR = Path(__file__).resolve().parent

# Wire up helper modules
db_module.init_db_path(DB_FILE)
show_utils.init_paths(CAPTURES_DIR, CSV_FILE)
AUTO_UPDATE_SECONDS = int(os.environ.get("TT_AUTO_UPDATE_SECONDS", "120"))
AI_DEFAULT_DAYS = int(os.environ.get("TT_AI_DAYS", "3"))
AI_MIN_CONFIDENCE = float(os.environ.get("TT_AI_MIN_CONFIDENCE", "0.20"))
AI_TOP_K = int(os.environ.get("TT_AI_TOP_K", "5"))
AI_SCORE_GAP = float(os.environ.get("TT_AI_SCORE_GAP", "0.0"))


def is_worktree_clean():
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
        )
        return result.returncode == 0 and result.stdout.strip() == ""
    except Exception:
        return False


def is_recording_running():
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
        return "tt_monitor.py" in result.stdout and "python" in result.stdout
    except Exception:
        return False


def auto_update_loop():
    while True:
        time.sleep(AUTO_UPDATE_SECONDS)
        if is_recording_running():
            continue
        if not is_worktree_clean():
            continue
        try:
            subprocess.run(
                ["git", "pull", "--ff-only"],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
            )
        except Exception:
            continue


def start_auto_update_thread():
    # Avoid double threads with the Flask reloader
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    thread = threading.Thread(target=auto_update_loop, daemon=True)
    thread.start()


def _get_table_columns(cursor, table_name):
    """Get column names for a table (works on both SQLite and PostgreSQL)."""
    if is_postgres():
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,),
        )
        return [row[0] for row in cursor.fetchall()]
    else:
        cursor.execute(f"PRAGMA table_info({table_name})")
        return [col[1] for col in cursor.fetchall()]


def _table_exists(cursor, table_name):
    """Check if a table exists (works on both SQLite and PostgreSQL)."""
    if is_postgres():
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
            (table_name,),
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None


def _safe_add_column(cursor, conn, table_name, col_name, col_type):
    """Add a column if it doesn't exist (works on both SQLite and PostgreSQL)."""
    columns = _get_table_columns(cursor, table_name)
    if col_name not in columns:
        print(f"Migrating database: adding {col_name} column to {table_name}...")
        placeholder = "%s" if is_postgres() else "?"
        # Can't parameterize DDL, but these values are hardcoded (safe)
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
        conn.commit()


def init_db():
    """Initialize database (SQLite or PostgreSQL)."""
    if is_postgres():
        import psycopg2
        conn = psycopg2.connect(db_module.DATABASE_URL)
    else:
        conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # --- Use SERIAL for PostgreSQL, AUTOINCREMENT for SQLite ---
    if is_postgres():
        auto_id = "SERIAL PRIMARY KEY"
        blob_type = "BYTEA"
    else:
        auto_id = "INTEGER PRIMARY KEY AUTOINCREMENT"
        blob_type = "BLOB"

    # Core tables
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS orgs (
            id {auto_id},
            name TEXT NOT NULL,
            employee_visible_columns TEXT DEFAULT NULL,
            api_key TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS users (
            id {auto_id},
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            approved INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS memberships (
            user_id INTEGER NOT NULL,
            org_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'employee',
            PRIMARY KEY (user_id, org_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS invites (
            id {auto_id},
            email TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'employee',
            token TEXT UNIQUE NOT NULL,
            org_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used_at TIMESTAMP DEFAULT NULL,
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS shows (
            id {auto_id},
            org_id INTEGER,
            name TEXT NOT NULL,
            date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
            item_name TEXT,
            show_id INTEGER,
            org_id INTEGER,
            cost REAL DEFAULT NULL,
            preset_name TEXT DEFAULT NULL,
            sku TEXT DEFAULT NULL,
            notes TEXT DEFAULT NULL,
            buyer TEXT DEFAULT NULL,
            order_id TEXT DEFAULT NULL,
            cancelled_status TEXT DEFAULT NULL,
            sold_price TEXT DEFAULT NULL,
            sold_timestamp TEXT DEFAULT NULL,
            viewers TEXT DEFAULT NULL,
            filename TEXT DEFAULT NULL,
            pinned_message TEXT DEFAULT NULL,
            PRIMARY KEY (item_name, show_id),
            FOREIGN KEY (show_id) REFERENCES shows(id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS presets (
            id {auto_id},
            org_id INTEGER,
            name TEXT UNIQUE NOT NULL,
            cost REAL NOT NULL,
            group_id INTEGER DEFAULT NULL,
            is_giveaway INTEGER DEFAULT 0,
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS preset_groups (
            id {auto_id},
            org_id INTEGER,
            name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS preset_group_links (
            preset_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            org_id INTEGER,
            PRIMARY KEY (preset_id, group_id),
            FOREIGN KEY (preset_id) REFERENCES presets(id),
            FOREIGN KEY (group_id) REFERENCES preset_groups(id),
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS recording_sessions (
            id {auto_id},
            show_id INTEGER NOT NULL,
            org_id INTEGER,
            started_at TEXT NOT NULL,
            stopped_at TEXT DEFAULT NULL,
            FOREIGN KEY (show_id) REFERENCES shows(id),
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS sku_presets (
            sku TEXT PRIMARY KEY,
            org_id INTEGER,
            preset_name TEXT DEFAULT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES orgs(id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS image_embeddings (
            id {auto_id},
            image_path TEXT NOT NULL,
            show_id INTEGER,
            org_id INTEGER,
            item_name TEXT,
            sku TEXT,
            preset_name TEXT,
            embedding {blob_type} NOT NULL,
            embedding_dim INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(image_path, show_id)
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS ai_feedback (
            id {auto_id},
            source_image_path TEXT NOT NULL,
            matched_image_path TEXT NOT NULL,
            org_id INTEGER,
            action TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_image_path, matched_image_path, action)
        )
    """)
    # Error log written to by the Chrome extension when a capture upload fails,
    # the video feed looks frozen, or the tab had to be auto-reloaded. Makes
    # Render incidents visible server-side without needing browser devtools.
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS extension_errors (
            id {auto_id},
            context TEXT,
            details TEXT,
            client_ts TEXT,
            dashboard_url TEXT,
            show_id INTEGER,
            item_title TEXT,
            user_agent TEXT,
            remote_addr TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_image_embeddings_show_id ON image_embeddings(show_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_image_embeddings_sku ON image_embeddings(sku)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_image_embeddings_preset ON image_embeddings(preset_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ai_feedback_source ON ai_feedback(source_image_path)")
    conn.commit()

    # --- Column migrations (run on BOTH SQLite and PostgreSQL) ---
    # _safe_add_column is a no-op if the column already exists, so this is safe
    # to run every startup. Previously gated behind `if not is_postgres()`, which
    # left Render's PG database missing the `image_data` column and caused
    # /api/items and item inserts to fail after CSV ephemeral disk was wiped.
    _safe_add_column(cursor, conn, "items", "preset_name", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "sku", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "notes", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "buyer", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "order_id", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "cancelled_status", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "sold_price", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "sold_timestamp", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "viewers", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "filename", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "pinned_message", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "items", "org_id", "INTEGER")
    # Image data stored directly in DB (for ephemeral-disk deployments like Render)
    image_col_type = "BYTEA" if is_postgres() else "BLOB"
    _safe_add_column(cursor, conn, "items", "image_data", image_col_type)
    _safe_add_column(cursor, conn, "shows", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "presets", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "presets", "group_id", "INTEGER DEFAULT NULL")
    _safe_add_column(cursor, conn, "presets", "is_giveaway", "INTEGER DEFAULT 0")
    _safe_add_column(cursor, conn, "preset_groups", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "preset_group_links", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "sku_presets", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "image_embeddings", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "ai_feedback", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "recording_sessions", "org_id", "INTEGER")
    _safe_add_column(cursor, conn, "users", "approved", "INTEGER NOT NULL DEFAULT 0")
    _safe_add_column(cursor, conn, "orgs", "employee_visible_columns", "TEXT DEFAULT NULL")
    _safe_add_column(cursor, conn, "orgs", "api_key", "TEXT DEFAULT NULL")

    # --- Ensure default org + owner user exist ---
    p = "%s" if is_postgres() else "?"

    cursor.execute("SELECT id FROM orgs ORDER BY id LIMIT 1")
    org_row = cursor.fetchone()
    if not org_row:
        cursor.execute(f"INSERT INTO orgs (name) VALUES ({p})", ("Default Org",))
        conn.commit()
        cursor.execute("SELECT id FROM orgs ORDER BY id LIMIT 1")
        org_row = cursor.fetchone()
    default_org_id = org_row[0]

    owner_email = os.environ.get("OWNER_EMAIL", "owner@example.com")
    owner_password = os.environ.get("OWNER_PASSWORD", "change-me")
    cursor.execute(f"SELECT id FROM users WHERE email = {p}", (owner_email,))
    user_row = cursor.fetchone()
    if not user_row:
        cursor.execute(
            f"INSERT INTO users (email, password_hash, approved) VALUES ({p}, {p}, 1)",
            (owner_email, generate_password_hash(owner_password, method="pbkdf2:sha256")),
        )
        conn.commit()
        cursor.execute(f"SELECT id FROM users WHERE email = {p}", (owner_email,))
        user_row = cursor.fetchone()
    owner_user_id = user_row[0]

    if is_postgres():
        cursor.execute(
            "INSERT INTO memberships (user_id, org_id, role) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (owner_user_id, default_org_id, "owner"),
        )
    else:
        cursor.execute(
            "INSERT OR IGNORE INTO memberships (user_id, org_id, role) VALUES (?, ?, ?)",
            (owner_user_id, default_org_id, "owner"),
        )
    conn.commit()

    # Ensure org has an API key
    cursor.execute(f"SELECT api_key FROM orgs WHERE id = {p}", (default_org_id,))
    api_key_row = cursor.fetchone()
    if not api_key_row or not api_key_row[0]:
        import secrets
        api_key = secrets.token_urlsafe(32)
        cursor.execute(f"UPDATE orgs SET api_key = {p} WHERE id = {p}", (api_key, default_org_id))
        conn.commit()
        print(f"Generated API key for org {default_org_id}: {api_key}")

    # Backfill org_id columns where missing
    for table in ["shows", "presets", "preset_groups", "preset_group_links",
                   "sku_presets", "image_embeddings", "ai_feedback",
                   "recording_sessions", "items"]:
        if _table_exists(cursor, table):
            cursor.execute(f"UPDATE {table} SET org_id = {p} WHERE org_id IS NULL", (default_org_id,))
    conn.commit()
    conn.close()


def upsert_sku_preset(sku: str, preset_name: str):
    if not sku or not preset_name:
        return
    org_id = get_current_org_id()
    execute(
        """INSERT OR REPLACE INTO sku_presets (sku, org_id, preset_name, updated_at)
           VALUES (?, ?, ?, CURRENT_TIMESTAMP)""",
        (sku, org_id, preset_name),
    )


def get_item_cost(item_name, show_id=None):
    """Get stored cost for an item."""
    org_id = get_current_org_id()
    if show_id:
        row = fetch_one(
            "SELECT cost FROM items WHERE item_name = ? AND show_id = ? AND org_id = ?",
            (item_name, show_id, org_id),
        )
    else:
        row = fetch_one(
            "SELECT cost FROM items WHERE item_name = ? AND org_id = ?",
            (item_name, org_id),
        )
    return row[0] if row else None


def get_item_preset_name(item_name, show_id=None):
    """Get stored preset name for an item."""
    org_id = get_current_org_id()
    if show_id:
        row = fetch_one(
            "SELECT preset_name FROM items WHERE item_name = ? AND show_id = ? AND org_id = ?",
            (item_name, show_id, org_id),
        )
    else:
        row = fetch_one(
            "SELECT preset_name FROM items WHERE item_name = ? AND org_id = ?",
            (item_name, org_id),
        )
    return row[0] if row else None


def set_item_cost(
    item_name,
    cost=_UNSET,
    show_id=None,
    preset_name=_UNSET,
    sku=_UNSET,
    notes=_UNSET,
    buyer=_UNSET,
    order_id=_UNSET,
    cancelled_status=_UNSET,
):
    """Set cost and optionally preset_name, sku, notes, buyer, order_id, cancelled_status for an item."""
    org_id = get_current_org_id()
    fields = {
        "cost": cost, "preset_name": preset_name, "sku": sku,
        "notes": notes, "buyer": buyer, "order_id": order_id,
        "cancelled_status": cancelled_status,
    }
    field_names = list(fields.keys())

    with get_db() as (conn, cursor):
        # Preserve existing fields if not provided
        if show_id:
            cursor.execute(
                f"SELECT {', '.join(field_names)} FROM items WHERE item_name = ? AND show_id = ? AND org_id = ?",
                (item_name, show_id, org_id),
            )
        else:
            cursor.execute(
                f"SELECT {', '.join(field_names)} FROM items WHERE item_name = ? AND org_id = ?",
                (item_name, org_id),
            )
        existing = cursor.fetchone()

        # Merge: keep existing values for any field not explicitly passed
        for i, name in enumerate(field_names):
            if fields[name] is _UNSET:
                fields[name] = existing[i] if existing else None

        if show_id:
            # Use real upsert (ON CONFLICT DO UPDATE) instead of INSERT OR REPLACE.
            # INSERT OR REPLACE is DELETE + INSERT under the hood, which wipes
            # any column not in the VALUES list (sold_price, sold_timestamp,
            # filename, image_data, pinned_message, viewers) — so every
            # preset/cost write would destroy the recorded image and metadata.
            # ON CONFLICT only touches the listed columns, preserving the rest.
            cursor.execute(
                """INSERT INTO items
                     (item_name, show_id, org_id, cost, preset_name, sku, notes, buyer, order_id, cancelled_status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (item_name, show_id) DO UPDATE SET
                     org_id = EXCLUDED.org_id,
                     cost = EXCLUDED.cost,
                     preset_name = EXCLUDED.preset_name,
                     sku = EXCLUDED.sku,
                     notes = EXCLUDED.notes,
                     buyer = EXCLUDED.buyer,
                     order_id = EXCLUDED.order_id,
                     cancelled_status = EXCLUDED.cancelled_status""",
                (item_name, show_id, org_id, *[fields[n] for n in field_names]),
            )
        else:
            # No show_id — can't upsert because the items PK is (item_name, show_id)
            # and PostgreSQL rejects NULL values in the PK. Fall back to a pure
            # UPDATE of any existing row for this org/item; insert has no valid
            # target and is skipped. (SQLite's legacy behavior of inserting with
            # NULL show_id silently broke the multi-show model anyway.)
            set_clause = ", ".join(f"{n} = ?" for n in field_names)
            cursor.execute(
                f"UPDATE items SET {set_clause} WHERE item_name = ? AND org_id = ?",
                (*[fields[n] for n in field_names], item_name, org_id),
            )
        conn.commit()

    # Keep SKU -> preset mapping in sync
    upsert_sku_preset(fields["sku"], fields["preset_name"])


_CLIP_MODEL = None
_CLIP_PREPROCESS = None
_CLIP_DEVICE = None


def get_clip_resources():
    global _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_DEVICE
    if _CLIP_MODEL is not None:
        return _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_DEVICE
    try:
        import torch
        import open_clip
    except Exception as exc:
        raise RuntimeError(
            "AI image matching needs extra packages. Run: pip install -r requirements.txt"
        ) from exc
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-B-32", pretrained="laion2b_s34b_b79k"
    )
    model = model.to(device)
    model.eval()
    _CLIP_MODEL = model
    _CLIP_PREPROCESS = preprocess
    _CLIP_DEVICE = device
    return _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_DEVICE


def normalize_embedding_path(image_path: Path) -> str:
    try:
        return str(image_path.relative_to(CAPTURES_DIR))
    except ValueError:
        return str(image_path)


def image_path_to_url(image_path: str) -> str:
    return f"/screenshots/{quote(image_path, safe='/')}"


def resolve_image_path(image_ref: str, show_id: Optional[int] = None) -> Optional[Path]:
    if not image_ref:
        return None
    if image_ref.startswith("/screenshots/"):
        relative = unquote(image_ref.replace("/screenshots/", "", 1))
        candidate = (CAPTURES_DIR / relative).resolve()
        try:
            candidate.relative_to(CAPTURES_DIR.resolve())
        except ValueError:
            return None
        return candidate
    if show_id:
        _, _, show_dir = get_show_info(show_id)
        if show_dir:
            candidate = (show_dir / image_ref).resolve()
            try:
                candidate.relative_to(show_dir.resolve())
            except ValueError:
                return None
            return candidate
    return Path(image_ref).resolve()


def load_items_meta(show_id: int) -> dict:
    org_id = get_current_org_id()
    rows = fetch_all(
        "SELECT item_name, sku, preset_name FROM items WHERE show_id = ? AND org_id = ?",
        (show_id, org_id),
    )
    return {row[0]: {"sku": row[1], "preset_name": row[2]} for row in rows}


def iter_show_items_for_ai(show_id: int, only_with_labels: bool = False):
    show_name, show_date, show_dir = get_show_info(show_id)
    if not show_dir:
        return []
    csv_file = show_dir / "log.csv"
    if not csv_file.exists():
        return []
    meta = load_items_meta(show_id)
    items = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            item_name = (row.get("item_title") or "").strip()
            if not item_name:
                continue
            filename = (row.get("filename") or "").strip()
            if not filename or filename == "__no_image__":
                continue
            image_path = show_dir / filename
            if not image_path.exists():
                continue
            item_meta = meta.get(item_name, {})
            sku = (item_meta.get("sku") or "").strip() if item_meta else ""
            preset_name = (
                (item_meta.get("preset_name") or "").strip() if item_meta else ""
            )
            if only_with_labels and not (sku or preset_name):
                continue
            items.append(
                {
                    "item_name": item_name,
                    "image_path": image_path,
                    "sku": sku or None,
                    "preset_name": preset_name or None,
                }
            )
    return items


def fetch_embedding(image_path_key: str, show_id: Optional[int]):
    org_id = get_current_org_id()
    if show_id is None:
        row = fetch_one(
            "SELECT embedding, embedding_dim FROM image_embeddings WHERE image_path = ? AND show_id IS NULL AND org_id = ?",
            (image_path_key, org_id),
        )
    else:
        row = fetch_one(
            "SELECT embedding, embedding_dim FROM image_embeddings WHERE image_path = ? AND show_id = ? AND org_id = ?",
            (image_path_key, show_id, org_id),
        )
    if not row:
        return None
    return row[0], row[1]


def upsert_embedding(
    image_path_key: str,
    show_id: Optional[int],
    item_name: Optional[str],
    sku: Optional[str],
    preset_name: Optional[str],
    embedding_bytes: bytes,
    embedding_dim: int,
):
    org_id = get_current_org_id()
    execute(
        """INSERT OR REPLACE INTO image_embeddings
           (image_path, show_id, org_id, item_name, sku, preset_name, embedding, embedding_dim)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (image_path_key, show_id, org_id, item_name, sku, preset_name, embedding_bytes, embedding_dim),
    )


def compute_image_embedding(image_path: Path):
    if not image_path.exists():
        return None
    model, preprocess, device = get_clip_resources()
    try:
        import torch
        import numpy as np
        from PIL import Image
    except Exception as exc:
        raise RuntimeError(
            "AI image matching needs Pillow, numpy, and torch. Run: pip install -r requirements.txt"
        ) from exc
    image = Image.open(image_path).convert("RGB")
    image_tensor = preprocess(image).unsqueeze(0).to(device)
    with torch.no_grad():
        embedding = model.encode_image(image_tensor)
        embedding = embedding / embedding.norm(dim=-1, keepdim=True)
    return embedding.cpu().numpy().astype("float32")[0]


def serialize_embedding(embedding_vector):
    import numpy as np

    vec = np.asarray(embedding_vector, dtype="float32")
    return vec.tobytes(), vec.shape[0]


def deserialize_embedding(embedding_bytes: bytes, embedding_dim: int):
    import numpy as np

    vec = np.frombuffer(embedding_bytes, dtype="float32")
    if embedding_dim and vec.shape[0] >= embedding_dim:
        vec = vec[:embedding_dim]
    return vec


def build_reference_embeddings(show_ids, force: bool = False):
    # Skip building from current TTL shows - use pre-imported WN companion embeddings only.
    # The WN embeddings are already in the database and don't need rebuilding.
    total = 0
    built = 0
    skipped = 0
    # Only build embeddings for QUERY items (items without labels) so they can be matched.
    # Reference embeddings come from the pre-imported WN companion data.
    for show_id in show_ids:
        items = iter_show_items_for_ai(show_id, only_with_labels=False)
        for item in items:
            total += 1
            image_path = item["image_path"]
            image_key = normalize_embedding_path(image_path)
            if not force and fetch_embedding(image_key, show_id):
                skipped += 1
                continue
            embedding = compute_image_embedding(image_path)
            if embedding is None:
                skipped += 1
                continue
            embedding_bytes, embedding_dim = serialize_embedding(embedding)
            # Store embedding WITHOUT preset/sku so it's a query, not a reference
            upsert_embedding(
                image_key,
                show_id,
                item["item_name"],
                None,  # No SKU - this is a query item, not reference
                None,  # No preset - this is a query item, not reference
                embedding_bytes,
                embedding_dim,
            )
            built += 1
    return {"total": total, "built": built, "skipped": skipped}


def load_reference_embeddings():
    org_id = get_current_org_id()
    rows = fetch_all(
        """SELECT image_path, show_id, item_name, sku, preset_name, embedding, embedding_dim
           FROM image_embeddings
           WHERE ((sku IS NOT NULL AND TRIM(sku) != '')
                  OR (preset_name IS NOT NULL AND TRIM(preset_name) != ''))
             AND org_id = ?""",
        (org_id,),
    )
    records = []
    embeddings = []
    for row in rows:
        image_path, show_id, item_name, sku, preset_name, blob, dim = row
        vec = deserialize_embedding(blob, dim)
        records.append(
            {
                "image_path": image_path,
                "show_id": show_id,
                "item_name": item_name,
                "sku": sku,
                "preset_name": preset_name,
            }
        )
        embeddings.append(vec)
    if not embeddings:
        return [], None
    import numpy as np

    matrix = np.vstack(embeddings).astype("float32")
    return records, matrix


def get_rejected_matches_for_source(source_image_path: str):
    org_id = get_current_org_id()
    rows = fetch_all(
        "SELECT matched_image_path FROM ai_feedback WHERE source_image_path = ? AND action = 'reject' AND org_id = ?",
        (source_image_path, org_id),
    )
    return {row[0] for row in rows}


def get_item_fields_map(show_id: int, item_names: list):
    if not item_names:
        return {}
    org_id = get_current_org_id()
    placeholders = ",".join(["?"] * len(item_names))
    rows = fetch_all(
        f"SELECT item_name, sku, preset_name FROM items WHERE show_id = ? AND org_id = ? AND item_name IN ({placeholders})",
        [show_id, org_id] + item_names,
    )
    return {row[0]: {"sku": row[1], "preset_name": row[2]} for row in rows}


def get_preset_cost_map():
    org_id = get_current_org_id()
    rows = fetch_all("SELECT name, cost FROM presets WHERE org_id = ?", (org_id,))
    return {row[0]: row[1] for row in rows}


def get_preset_giveaway_map():
    org_id = get_current_org_id()
    rows = fetch_all("SELECT name, is_giveaway FROM presets WHERE org_id = ?", (org_id,))
    return {row[0]: bool(row[1]) for row in rows if row[0]}


def get_sku_preset_map():
    org_id = get_current_org_id()
    rows = fetch_all("SELECT sku, preset_name FROM sku_presets WHERE org_id = ?", (org_id,))
    return {row[0]: row[1] for row in rows if row[0] and row[1]}


def normalize_item_name(name):
    """Normalize item name for matching."""
    if not name:
        return ""
    # Convert to lowercase
    normalized = name.lower()
    # Replace underscores with spaces (for filename matching)
    normalized = normalized.replace("_", " ")
    # Remove special characters except spaces, lowercase, strip
    normalized = re.sub(r"[^a-zA-Z0-9\s]", "", normalized)
    # Remove extra spaces
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def parse_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))
    if cleaned == "":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def get_show_info(show_id, org_id=None):
    if org_id is None:
        try:
            org_id = get_current_org_id()
        except Exception:
            org_id = None
    return show_utils.get_show_info(show_id, org_id)


def load_show_rows(show_id):
    show_name, show_date, show_dir = get_show_info(show_id)
    if not show_dir:
        return None, None, None, None
    csv_file = show_dir / "log.csv"
    if not csv_file.exists():
        return show_name, show_date, show_dir, []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]
    return show_name, show_date, show_dir, rows


def compute_show_summary(show_id, commission_rate=0.08, processed_only=False):
    show_name, show_date, show_dir, rows = load_show_rows(show_id)
    if show_name is None:
        return None

    sold_prices = []
    sold_times = []
    viewers = []
    items_sold = 0

    # Load costs + cancelled/failed counts from DB
    org_id = get_current_org_id()
    rows_db = fetch_all(
        "SELECT item_name, cost, cancelled_status FROM items WHERE show_id = ? AND org_id = ?",
        (show_id, org_id),
    )

    status_map = {}
    total_cost = 0.0
    items_with_cost = 0
    cancelled_count = 0
    failed_count = 0
    for item_name, cost, cancelled_status in rows_db:
        status_map[item_name] = cancelled_status
        if cancelled_status == "Cancelled":
            cancelled_count += 1
        if cancelled_status == "Failed":
            failed_count += 1
        if cost is not None and (not processed_only or cancelled_status in (None, "")):
            total_cost += cost
            items_with_cost += 1

    for row in rows:
        sold_price = parse_number(row.get("sold_price"))
        sold_timestamp = parse_timestamp(row.get("sold_timestamp"))
        item_name = (row.get("item_title") or "").strip()
        if processed_only:
            status = status_map.get(item_name)
            if status in {"Cancelled", "Failed"}:
                continue
        if sold_price is not None or sold_timestamp is not None:
            items_sold += 1
        if sold_price is not None:
            sold_prices.append(sold_price)
        if sold_timestamp:
            sold_times.append(sold_timestamp)
        viewer_val = parse_number(row.get("viewers"))
        if viewer_val is not None:
            viewers.append(viewer_val)

    sold_times.sort()
    show_minutes = None
    avg_between_sales = None
    if len(sold_times) >= 2:
        show_seconds = (sold_times[-1] - sold_times[0]).total_seconds()
        show_minutes = show_seconds / 60.0 if show_seconds > 0 else None
        gaps = [
            (sold_times[i] - sold_times[i - 1]).total_seconds()
            for i in range(1, len(sold_times))
        ]
        avg_between_sales = sum(gaps) / len(gaps) if gaps else None

    total_revenue_pre_fees = sum(sold_prices)
    total_revenue_post_fees = 0.0
    for price in sold_prices:
        total_revenue_post_fees += price * (1 - commission_rate - 0.029) - 0.30

    total_profit = total_revenue_post_fees - total_cost
    aov = total_revenue_pre_fees / items_sold if items_sold else 0.0
    aoc = total_cost / items_with_cost if items_with_cost else 0.0
    avg_profit = total_profit / items_sold if items_sold else 0.0
    avg_viewers = sum(viewers) / len(viewers) if viewers else 0.0
    sales_per_minute = (
        items_sold / show_minutes if show_minutes and show_minutes > 0 else None
    )
    minutes_per_sale = (
        show_minutes / items_sold if show_minutes and items_sold else None
    )

    def format_minutes(value):
        if value is None:
            return "N/A"
        return f"{value:.2f}"

    def format_seconds(value):
        if value is None:
            return "N/A"
        return f"{value:.2f}"

    return {
        "show_id": show_id,
        "show_name": show_name,
        "show_date": show_date,
        "commission_rate": commission_rate,
        "items_sold": items_sold,
        "processed_only": processed_only,
        "show_minutes": show_minutes,
        "show_minutes_display": format_minutes(show_minutes),
        "avg_time_between_sales": avg_between_sales,
        "avg_time_between_sales_display": format_seconds(avg_between_sales),
        "total_revenue_pre_fees": total_revenue_pre_fees,
        "total_revenue_post_fees": total_revenue_post_fees,
        "total_cost": total_cost,
        "total_profit": total_profit,
        "aov": aov,
        "aoc": aoc,
        "avg_profit": avg_profit,
        "cancelled_count": cancelled_count,
        "failed_count": failed_count,
        "avg_viewers": avg_viewers,
        "sales_per_minute": sales_per_minute,
        "minutes_per_sale": minutes_per_sale,
    }


def find_image_for_item(item_name, search_dir=None):
    """Find screenshot image for an item based on normalized name matching."""
    if not item_name:
        return None
    
    # Use show_dir if provided, otherwise use CAPTURES_DIR
    if search_dir is None:
        search_dir = CAPTURES_DIR
    
    # Extract number from item name (e.g., "RANDOM PULL #216" -> "216")
    number_match = re.search(r"#(\d+)", item_name)
    item_number = number_match.group(1) if number_match else None
    
    # Normalize item name from CSV (has spaces, may have #)
    item_normalized = normalize_item_name(item_name)
    
    # Get key words from item name (remove numbers, very short words)
    item_words = set(item_normalized.split())
    item_words = {w for w in item_words if len(w) > 1 and not w.isdigit()}
    
    # Look for PNG files in search directory
    best_match = None
    best_score = 0
    
    if search_dir.exists():
        for img_file in search_dir.glob("*.png"):
            filename_normalized = normalize_item_name(img_file.stem)
            
            # Extract number from filename
            filename_number_match = re.search(r"(\d+)", img_file.stem)
            filename_number = (
                filename_number_match.group(1) if filename_number_match else None
            )
            
            score = 0
            
            # Bonus if item number matches filename number
            if item_number and filename_number and item_number == filename_number:
                score += 100
            
            # Count matching words
            filename_words = set(filename_normalized.split())
            filename_words = {w for w in filename_words if len(w) > 1}
            matching_words = item_words.intersection(filename_words)
            score += len(matching_words) * 10
            
            if score > best_score:
                best_score = score
                best_match = img_file.name
    
    return best_match


def fix_csv_file():
    """Ensure CSV file has proper structure (sold_price and sold_timestamp columns)."""
    if not CSV_FILE.exists():
        return
    
    try:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        if not rows:
            return
        
        header = rows[0]
        updated = False

        # Add sold_price column if missing
        if "sold_price" not in header:
            header.append("sold_price")
            updated = True

        # Add sold_timestamp column if missing
        if "sold_timestamp" not in header:
            header.append("sold_timestamp")
            updated = True

        # Add viewers column if missing
        if "viewers" not in header:
            header.append("viewers")
            updated = True

        if updated:
            rows[0] = header
            
            # Ensure all rows have the same number of columns
            num_columns = len(header)
            for i in range(1, len(rows)):
                while len(rows[i]) < num_columns:
                    rows[i].append("")
            
            with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(rows)
    except Exception:
        pass  # Ignore errors during fix


def get_current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    row = fetch_one("SELECT id, email, approved FROM users WHERE id = ?", (user_id,))
    if not row:
        return None
    return {"id": row[0], "email": row[1], "approved": row[2]}


def get_current_org_id():
    org_id = session.get("org_id")
    if org_id:
        return org_id
    user = get_current_user()
    if not user:
        return None
    row = fetch_one(
        "SELECT org_id FROM memberships WHERE user_id = ? ORDER BY org_id LIMIT 1",
        (user["id"],),
    )
    if not row:
        return None
    session["org_id"] = row[0]
    return row[0]


def get_user_role(user_id: int, org_id: int):
    row = fetch_one(
        "SELECT role FROM memberships WHERE user_id = ? AND org_id = ?",
        (user_id, org_id),
    )
    return row[0] if row else None


def require_owner():
    user = get_current_user()
    if not user:
        return False
    org_id = get_current_org_id()
    if not org_id:
        return False
    role = get_user_role(user["id"], org_id)
    return role == "owner"


def get_invite_by_token(token: str):
    row = fetch_one("SELECT id, email, role, org_id, used_at FROM invites WHERE token = ?", (token,))
    if not row:
        return None
    return {"id": row[0], "email": row[1], "role": row[2], "org_id": row[3], "used_at": row[4]}


def get_invite_by_email(email: str):
    row = fetch_one(
        "SELECT id, email, role, org_id, used_at FROM invites WHERE lower(email) = lower(?) AND used_at IS NULL ORDER BY created_at DESC LIMIT 1",
        (email,),
    )
    if not row:
        return None
    return {"id": row[0], "email": row[1], "role": row[2], "org_id": row[3], "used_at": row[4]}


def mark_invite_used(invite_id: int):
    execute("UPDATE invites SET used_at = ? WHERE id = ?", (datetime.now().isoformat(), invite_id))


def create_user(email: str, password: str, approved: int = 0):
    return execute_returning_id(
        "INSERT INTO users (email, password_hash, approved) VALUES (?, ?, ?)",
        (email, generate_password_hash(password, method="pbkdf2:sha256"), approved),
    )


def login_required(view_func):
    def wrapped(*args, **kwargs):
        user = get_current_user()
        if not user:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Unauthorized"}), 401
            return redirect(url_for("login"))
        # Check if user is approved
        if not user.get("approved"):
            session.clear()
            if request.path.startswith("/api/"):
                return jsonify({"error": "Account pending approval"}), 403
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    wrapped.__name__ = view_func.__name__
    return wrapped


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")
    data = request.form or {}
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    if not email or not password:
        return render_template("login.html", error="Email and password required")
    row = fetch_one("SELECT id, password_hash, approved FROM users WHERE email = ?", (email,))
    if not row or not check_password_hash(row[1], password):
        return render_template("login.html", error="Invalid credentials")
    user_id = row[0]
    approved = row[2]
    if not approved:
        return render_template("login.html", error="Your account is pending approval. The owner will approve it soon.")
    membership = fetch_one(
        "SELECT org_id FROM memberships WHERE user_id = ? ORDER BY org_id LIMIT 1",
        (user_id,),
    )
    if not membership:
        return render_template("login.html", error="No organization access")
    session["user_id"] = user_id
    session["org_id"] = membership[0]
    return redirect(url_for("index"))


@app.route("/login/google")
def login_google():
    if not app.config.get("GOOGLE_CLIENT_ID") or not app.config.get("GOOGLE_CLIENT_SECRET"):
        return render_template("login.html", error="Google login not configured")
    redirect_uri = url_for("google_callback", _external=True)
    return oauth.google.authorize_redirect(redirect_uri)


@app.route("/auth/google")
def google_callback():
    if not app.config.get("GOOGLE_CLIENT_ID") or not app.config.get("GOOGLE_CLIENT_SECRET"):
        return render_template("login.html", error="Google login not configured")
    token = oauth.google.authorize_access_token()
    userinfo = token.get("userinfo")
    if not userinfo:
        userinfo = oauth.google.get("userinfo").json()
    email = (userinfo.get("email") or "").lower()
    if not email:
        return render_template("login.html", error="Google login failed")

    row = fetch_one("SELECT id, approved FROM users WHERE email = ?", (email,))

    if row:
        user_id = row[0]
        if not row[1]:
            return render_template("login.html", error="Your account is pending approval. The owner will approve it soon.")
        membership = fetch_one(
            "SELECT org_id FROM memberships WHERE user_id = ? ORDER BY org_id LIMIT 1",
            (user_id,),
        )
        if not membership:
            return render_template("login.html", error="No organization access")
        session["user_id"] = user_id
        session["org_id"] = membership[0]
        return redirect(url_for("index"))

    invite = get_invite_by_email(email)
    if not invite:
        return render_template("login.html", error="No invite found for this email")

    user_id = create_user(email, secrets.token_urlsafe(16), approved=1)
    execute(
        "INSERT OR IGNORE INTO memberships (user_id, org_id, role) VALUES (?, ?, ?)",
        (user_id, invite["org_id"], invite["role"]),
    )
    mark_invite_used(invite["id"])
    session["user_id"] = user_id
    session["org_id"] = invite["org_id"]
    return redirect(url_for("index"))


@app.route("/invite/<token>", methods=["GET", "POST"])
def accept_invite(token):
    invite = get_invite_by_token(token)
    if not invite or invite["used_at"]:
        return render_template("invite.html", error="Invite is invalid or already used")
    if request.method == "GET":
        return render_template("invite.html", email=invite["email"], role=invite["role"])

    data = request.form or {}
    password = (data.get("password") or "").strip()
    if not password:
        return render_template("invite.html", error="Password required", email=invite["email"], role=invite["role"])

    with get_db() as (conn, cursor):
        cursor.execute(translate("SELECT id FROM users WHERE email = ?"), (invite["email"],))
        row = cursor.fetchone()
        if row:
            user_id = row[0]
            cursor.execute(
                "UPDATE users SET password_hash = ?, approved = 1 WHERE id = ?",
                (generate_password_hash(password, method="pbkdf2:sha256"), user_id),
            )
        else:
            if is_postgres():
                cursor.execute(
                    "INSERT INTO users (email, password_hash, approved) VALUES (%s, %s, 1) RETURNING id",
                    (invite["email"], generate_password_hash(password, method="pbkdf2:sha256")),
                )
                user_id = cursor.fetchone()[0]
            else:
                cursor.execute(
                    "INSERT INTO users (email, password_hash, approved) VALUES (?, ?, 1)",
                    (invite["email"], generate_password_hash(password, method="pbkdf2:sha256")),
                )
                user_id = cursor.lastrowid
        cursor.execute(
            "INSERT OR IGNORE INTO memberships (user_id, org_id, role) VALUES (?, ?, ?)",
            (user_id, invite["org_id"], invite["role"]),
        )
        conn.commit()
    mark_invite_used(invite["id"])
    session["user_id"] = user_id
    session["org_id"] = invite["org_id"]
    return redirect(url_for("index"))


@app.route("/invites", methods=["GET", "POST"])
@login_required
def manage_invites():
    if not require_owner():
        return "Forbidden", 403

    org_id = get_current_org_id()
    if request.method == "POST":
        data = request.form or {}
        email = (data.get("email") or "").strip().lower()
        role = (data.get("role") or "employee").strip().lower()
        if not email:
            return render_template("invites.html", error="Email required")
        if role not in {"owner", "employee"}:
            role = "employee"
        token = secrets.token_urlsafe(24)
        execute(
            "INSERT INTO invites (email, role, token, org_id) VALUES (?, ?, ?, ?)",
            (email, role, token, org_id),
        )

    invites = fetch_all(
        "SELECT email, role, token, created_at, used_at FROM invites WHERE org_id = ? ORDER BY created_at DESC",
        (org_id,),
    )
    return render_template(
        "invites.html",
        invites=invites,
        base_url=request.url_root.rstrip("/"),
    )


# Default columns employees can see (Item # and Image)
# --- Email config (set these in your environment or .env) ---
SMTP_HOST = os.environ.get("SMTP_HOST", "")        # e.g. smtp.gmail.com
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587")) # 587 for TLS
SMTP_USER = os.environ.get("SMTP_USER", "")         # your email address
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")  # app password (not your regular password)
FROM_EMAIL = os.environ.get("FROM_EMAIL", SMTP_USER) # "from" address on emails


def send_invite_email(to_email: str, invite_link: str):
    """Send an invite email to a new employee. Returns (success, error_message)."""
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASSWORD:
        return False, "Email not configured (set SMTP_HOST, SMTP_USER, SMTP_PASSWORD)"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "You're invited to AuctionOps"
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email

    # Plain text version
    text = f"""You've been invited to join AuctionOps!

Click the link below to create your account:
{invite_link}

If you didn't expect this invite, you can ignore this email."""

    # HTML version (looks nicer in email clients)
    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; max-width: 500px; margin: 0 auto; padding: 20px;">
    <div style="background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 30px; color: #e5e7eb;">
        <h1 style="margin: 0 0 10px; font-size: 20px; color: #fff;">You're invited to AuctionOps</h1>
        <p style="color: #9ca3af; font-size: 14px; margin-bottom: 24px;">You've been invited to join the team. Click below to create your account and get started.</p>
        <a href="{invite_link}" style="display: inline-block; background: #3b82f6; color: #fff; text-decoration: none; padding: 12px 24px; border-radius: 8px; font-weight: 600; font-size: 14px;">Create Account</a>
        <p style="color: #6b7280; font-size: 12px; margin-top: 24px;">Or copy this link: {invite_link}</p>
    </div>
</div>"""

    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(FROM_EMAIL, to_email, msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)


DEFAULT_EMPLOYEE_COLUMNS = ["row_number", "image"]

# All available columns for the permission toggles
ALL_COLUMNS = [
    ("row_number", "Item #"),
    ("image", "Image"),
    ("item_name", "Product Name"),
    ("sold_timestamp", "Sold Time"),
    ("viewers", "Viewers"),
    ("order_id", "Order ID"),
    ("buyer", "Buyer"),
    ("cancelled_status", "Cancelled/Failed"),
    ("sku", "SKU"),
    ("notes", "Notes"),
    ("preset", "Preset"),
    ("pinned_message", "Pinned Message"),
    ("sold_price", "Sold Price"),
    ("net_revenue", "Net Revenue"),
    ("cost", "Cost"),
    ("profit", "Profit"),
]


def get_employee_visible_columns(org_id):
    """Get the list of column IDs employees can see for this org."""
    row = fetch_one("SELECT employee_visible_columns FROM orgs WHERE id = ?", (org_id,))
    if row and row[0]:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            pass
    return DEFAULT_EMPLOYEE_COLUMNS


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings_page():
    """Settings & Permissions page — owner only."""
    if not require_owner():
        return "Forbidden", 403

    org_id = get_current_org_id()
    success = request.args.get("success")
    error = request.args.get("error")

    # Handle invite creation
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        if not email:
            error = "Email is required"
        else:
            # Check if user already exists
            existing = fetch_one("SELECT id FROM users WHERE email = ?", (email,))
            if existing:
                error = "A user with that email already exists"
            else:
                token = secrets.token_urlsafe(24)
                execute(
                    "INSERT INTO invites (email, role, token, org_id) VALUES (?, ?, ?, ?)",
                    (email, "employee", token, org_id),
                )
                invite_link = request.url_root.rstrip("/") + f"/invite/{token}"
                email_sent, email_error = send_invite_email(email, invite_link)
                if email_sent:
                    success = f"Invite sent to {email}!"
                else:
                    success = f"Invite created! (Email not sent: {email_error}) Share the link manually."

    # Get invites list
    invites = fetch_all(
        "SELECT email, role, token, created_at, used_at FROM invites WHERE org_id = ? ORDER BY created_at DESC",
        (org_id,),
    )

    # Get team members (users with memberships in this org)
    team_members = []
    rows = fetch_all(
        """SELECT u.id, u.email, m.role, u.approved
           FROM users u
           JOIN memberships m ON u.id = m.user_id
           WHERE m.org_id = ?
           ORDER BY m.role DESC, u.email""",
        (org_id,),
    )
    for r in rows:
        team_members.append({"user_id": r[0], "email": r[1], "role": r[2], "approved": r[3]})

    employee_columns = get_employee_visible_columns(org_id)

    # Fetch this org's API key so we can show it on the settings page.
    # If one doesn't exist yet (e.g. old org), mint one so the UI is never empty.
    api_key_row = fetch_one("SELECT api_key FROM orgs WHERE id = ?", (org_id,))
    api_key = (api_key_row[0] if api_key_row else None) or ""
    if not api_key:
        api_key = secrets.token_urlsafe(32)
        execute("UPDATE orgs SET api_key = ? WHERE id = ?", (api_key, org_id))

    current_user = get_current_user()
    return render_template(
        "settings.html",
        invites=invites,
        team_members=team_members,
        base_url=request.url_root.rstrip("/"),
        employee_columns=employee_columns,
        all_columns=ALL_COLUMNS,
        current_user_id=current_user["id"] if current_user else None,
        api_key=api_key,
        error=error,
        success=success,
    )


@app.route("/settings/approve", methods=["POST"])
@login_required
def approve_user():
    """Approve a pending user account."""
    if not require_owner():
        return "Forbidden", 403
    user_id = request.form.get("user_id")
    if user_id:
        execute("UPDATE users SET approved = 1 WHERE id = ?", (int(user_id),))
    return redirect(url_for("settings_page", success="User approved"))


@app.route("/settings/remove", methods=["POST"])
@login_required
def remove_user():
    """Remove a user from the org."""
    if not require_owner():
        return "Forbidden", 403
    user_id = request.form.get("user_id")
    org_id = get_current_org_id()
    if user_id:
        user_id = int(user_id)
        # Don't allow removing yourself
        current = get_current_user()
        if current and current["id"] == user_id:
            return redirect(url_for("settings_page", error="You cannot remove yourself"))
        execute("DELETE FROM memberships WHERE user_id = ? AND org_id = ?", (user_id, org_id))
    return redirect(url_for("settings_page", success="User removed"))


@app.route("/settings/columns", methods=["POST"])
@login_required
def save_column_permissions():
    """Save which columns employees can see."""
    if not require_owner():
        return "Forbidden", 403
    org_id = get_current_org_id()
    # row_number and image are always included
    selected = request.form.getlist("columns")
    # Ensure defaults are always present
    for col in DEFAULT_EMPLOYEE_COLUMNS:
        if col not in selected:
            selected.insert(0, col)
    execute(
        "UPDATE orgs SET employee_visible_columns = ? WHERE id = ?",
        (json.dumps(selected), org_id),
    )
    return redirect(url_for("settings_page", success="Column permissions saved"))


@app.route("/register", methods=["GET", "POST"])
def register():
    """Account registration — creates a pending account that needs owner approval."""
    if request.method == "GET":
        return render_template("register.html")
    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "").strip()
    confirm = (request.form.get("confirm_password") or "").strip()
    if not email or not password:
        return render_template("register.html", error="Email and password are required")
    if len(password) < 6:
        return render_template("register.html", error="Password must be at least 6 characters")
    if password != confirm:
        return render_template("register.html", error="Passwords do not match")
    # Check if email already exists
    existing = fetch_one("SELECT id FROM users WHERE email = ?", (email,))
    if existing:
        return render_template("register.html", error="An account with that email already exists")
    # Create the user as pending (approved=0)
    user_id = create_user(email, password, approved=0)
    # Check if there's an invite for this email — if so, auto-approve and assign to org
    invite = get_invite_by_email(email)
    if invite:
        execute("UPDATE users SET approved = 1 WHERE id = ?", (user_id,))
        execute(
            "INSERT OR IGNORE INTO memberships (user_id, org_id, role) VALUES (?, ?, ?)",
            (user_id, invite["org_id"], invite["role"]),
        )
        mark_invite_used(invite["id"])
        session["user_id"] = user_id
        session["org_id"] = invite["org_id"]
        return redirect(url_for("index"))
    # No invite — account is pending, they need to be approved + added to an org
    return render_template("register.html", success="Account created! Ask the owner to approve your account.")


@app.route("/api/user-info", methods=["GET"])
@login_required
def user_info():
    """Return current user info including role and visible columns."""
    user = get_current_user()
    org_id = get_current_org_id()
    role = get_user_role(user["id"], org_id) if user and org_id else None
    visible_columns = None
    if role and role != "owner":
        visible_columns = get_employee_visible_columns(org_id)
    return jsonify({
        "email": user["email"],
        "role": role or "employee",
        "visible_columns": visible_columns,
    })


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    fix_csv_file()
    return render_template("index.html")


@app.route("/presets")
@login_required
def presets_page():
    """Presets management page (owner only)."""
    if not require_owner():
        return redirect(url_for("index"))
    return render_template("presets.html")


@app.route("/skus")
@login_required
def skus_page():
    """SKU management page (owner only)."""
    if not require_owner():
        return redirect(url_for("index"))
    return render_template("skus.html")


@app.route("/giveaways")
@login_required
def giveaways_page():
    """Giveaway insights page (owner only)."""
    if not require_owner():
        return redirect(url_for("index"))
    return render_template("giveaways.html")


@app.route("/profit-loss")
@login_required
def profit_loss_page():
    """Profit & loss page (owner only)."""
    if not require_owner():
        return redirect(url_for("index"))
    return render_template("profit_loss.html")


@app.route("/api/debug/images")
def debug_images():
    """Debug endpoint to see image matching."""
    debug_info = []
    
    if CSV_FILE.exists():
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                item_name = row.get("item_title", "").strip()
                filename = row.get("filename", "").strip()
                
                image_path = None
                if filename:
                    img_file = CAPTURES_DIR / filename
                    if img_file.exists():
                        image_path = f"/screenshots/{filename}"
                
                if not image_path:
                    matched = find_image_for_item(item_name)
                    if matched:
                        image_path = f"/screenshots/{matched}"
                
                debug_info.append(
                    {
                        "item_name": item_name,
                        "csv_filename": filename,
                        "matched_filename": find_image_for_item(item_name),
                        "image_path": image_path,
                    }
                )
    
    return jsonify(debug_info)


@app.route("/api/items")
def get_items():
    """Get all items from CSV with stored costs and image paths."""
    items = []
    
    # Get show_id from query parameter
    show_id = request.args.get("show_id", type=int)
    org_id = get_current_org_id()
    
    if not show_id:
        return jsonify(items)
    
    # Get show info from database to find the show folder
    result = fetch_one(
        "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
        (show_id, org_id),
    )

    if not result:
        return jsonify(items)

    show_name, show_date = result

    # Create show-specific folder path
    show_dir = show_dir_path(show_name, show_date)
    csv_file = show_dir / "log.csv"

    if not csv_file.exists():
        # No CSV on disk — load items from database (e.g. on Render server)
        db_items = fetch_all(
            """SELECT item_name, cost, preset_name, sku, notes, buyer, order_id,
                      cancelled_status, sold_price, sold_timestamp, viewers, filename, pinned_message,
                      CASE WHEN image_data IS NOT NULL THEN 1 ELSE 0 END
               FROM items WHERE show_id = ? AND org_id = ? ORDER BY item_name""",
            (show_id, org_id),
        )
        from urllib.parse import quote as _urlquote
        for r in db_items:
            sold_price_float = None
            if r[8]:
                price_match = re.search(r"[\d,]+\.?\d*", str(r[8]))
                if price_match:
                    try:
                        sold_price_float = float(price_match.group().replace(",", ""))
                    except ValueError:
                        pass
            has_image = bool(r[13])
            image_url = f"/db-image/{show_id}/{_urlquote(r[0])}" if has_image else None
            items.append({
                "item_name": r[0], "cost": r[1], "preset_name": r[2], "sku": r[3],
                "notes": r[4], "buyer": r[5], "order_id": r[6], "cancelled_status": r[7],
                "sold_price": r[8] or "", "sold_price_float": sold_price_float,
                "sold_timestamp": r[9] or "", "viewers": r[10] or "",
                "filename": r[11] or "", "pinned_message": r[12] or "",
                "image": image_url, "timestamp": "",
            })
        return jsonify(items)

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            item_name = row.get("item_title", "").strip()
            if not item_name:
                continue

            def get_row_value(row_data, *keys):
                for key in keys:
                    value = row_data.get(key)
                    if value is not None:
                        return value
                lower_map = {
                    k.lower(): k for k in row_data.keys() if isinstance(k, str)
                }
                for key in keys:
                    lookup = lower_map.get(key.lower())
                    if lookup is not None:
                        return row_data.get(lookup)
                return ""

            pinned_message = (get_row_value(row, "pinned_text", "pinned message") or "").strip()
            sold_price = (
                get_row_value(row, "sold_price", "sold price", "soldprice", "price") or ""
            ).strip()
            sold_timestamp = (
                get_row_value(row, "sold_timestamp", "sold time", "sold_time") or ""
            ).strip()
            viewers = (get_row_value(row, "viewers", "viewer") or "").strip()
            filename = (get_row_value(row, "filename", "file", "image") or "").strip()
            row_timestamp = (get_row_value(row, "timestamp", "time") or "").strip()

            # Get stored cost, preset_name, sku, notes, buyer, order_id, cancelled_status
            cost = get_item_cost(item_name, show_id)
            preset_name = get_item_preset_name(item_name, show_id)
            
            # Get sku, notes, buyer, order_id, cancelled_status
            if show_id:
                result = fetch_one(
                    "SELECT sku, notes, buyer, order_id, cancelled_status FROM items WHERE item_name = ? AND show_id = ? AND org_id = ?",
                    (item_name, show_id, org_id),
                )
            else:
                result = fetch_one(
                    "SELECT sku, notes, buyer, order_id, cancelled_status FROM items WHERE item_name = ? AND org_id = ?",
                    (item_name, org_id),
                )
            sku = result[0] if result and result[0] else None
            notes = result[1] if result and result[1] else None
            buyer = result[2] if result and result[2] else None
            order_id = result[3] if result and result[3] else None
            cancelled_status = result[4] if result and result[4] else None
            
            # Find image - CSV has exact filename, use it directly
            image_path = None
            
            # Method 1: Direct filename match from CSV in show folder
            if filename:
                filename = filename.strip()
                if filename.startswith("http://") or filename.startswith("https://"):
                    image_path = filename
                elif filename.startswith("data:image"):
                    image_path = filename
                elif filename != "__no_image__":
                    img_file = show_dir / filename
                    if img_file.exists() and img_file.is_file():
                        # URL encode the path for browser (handles # and spaces)
                        from urllib.parse import quote

                        # Include show folder in path
                        image_path = quote(
                            f"/screenshots/{safe_filename(show_name)}_{safe_filename(show_date)}/{filename}", safe="/"
                        )

            # Method 2: Smart matching by item name in show folder
            # Only do this when there is no filename AND we are allowed to auto-match
            if not image_path and not filename:
                # Skip auto-matching for imports that explicitly disable images
                pass

            # Parse sold price to float
            sold_price_float = None
            if sold_price:
                # Extract number from price string like "$41"
                price_match = re.search(r"[\d,]+\.?\d*", sold_price)
                if price_match:
                    try:
                        sold_price_float = float(price_match.group().replace(",", ""))
                    except ValueError:
                        pass
            
            items.append(
                {
                    "item_name": item_name,
                    "pinned_message": pinned_message,
                    "timestamp": row_timestamp,
                    "filename": filename,
                    "sold_price": sold_price,
                    "sold_price_float": sold_price_float,
                    "sold_timestamp": sold_timestamp,
                    "cost": cost,
                    "preset_name": preset_name,
                    "sku": sku,
                    "notes": notes,
                    "buyer": buyer,
                    "order_id": order_id,
                    "cancelled_status": cancelled_status,
                    "viewers": viewers,
                    "image": image_path,
                }
            )
    
    return jsonify(items)


@app.route("/api/ai/build-index", methods=["POST"])
def ai_build_index():
    """Build or refresh image embeddings for recent shows."""
    data = request.json or {}
    show_ids = data.get("show_ids")
    force = bool(data.get("force", False))
    limit_shows = data.get("limit_shows")
    days = data.get("days", AI_DEFAULT_DAYS)
    org_id = get_current_org_id()

    if show_ids:
        try:
            selected_ids = [int(s) for s in show_ids if str(s).strip()]
        except ValueError:
            return jsonify({"error": "Invalid show_ids"}), 400
    else:
        selected_ids = []
        if limit_shows:
            selected_ids = [row[0] for row in fetch_all(
                "SELECT id FROM shows WHERE org_id = ? ORDER BY created_at DESC LIMIT ?",
                (org_id, int(limit_shows)),
            )]
        else:
            use_all = False
            try:
                use_all = int(days) <= 0
            except (TypeError, ValueError):
                use_all = False
            if use_all:
                selected_ids = [row[0] for row in fetch_all(
                    "SELECT id FROM shows WHERE org_id = ? ORDER BY created_at DESC",
                    (org_id,),
                )]
            else:
                cutoff = datetime.now() - timedelta(days=int(days))
                # Drop the SQLite-specific datetime() wrappers; both SQLite and
                # PostgreSQL compare TIMESTAMP columns to ISO-8601 strings
                # correctly without them. SQLite's datetime() has no direct
                # equivalent in PG and was breaking this endpoint on Render.
                selected_ids = [row[0] for row in fetch_all(
                    """
                    SELECT id FROM shows
                    WHERE org_id = ? AND created_at >= ?
                    ORDER BY created_at DESC
                """,
                    (org_id, cutoff.isoformat(sep=" "),),
                )]

        if not selected_ids:
            selected_ids = [row[0] for row in fetch_all(
                "SELECT id FROM shows WHERE org_id = ? ORDER BY created_at DESC LIMIT 3",
                (org_id,),
            )]

    if not selected_ids:
        return jsonify({"error": "No shows found to index"}), 400

    stats = build_reference_embeddings(selected_ids, force=force)
    return jsonify(
        {
            "show_ids": selected_ids,
            "total": stats["total"],
            "built": stats["built"],
            "skipped": stats["skipped"],
        }
    )


@app.route("/api/ai/run", methods=["POST"])
def ai_run():
    """Run AI image matching for the provided items."""
    data = request.json or {}
    show_id = data.get("show_id")
    if not show_id:
        return jsonify({"error": "show_id is required"}), 400
    try:
        show_id = int(show_id)
    except ValueError:
        return jsonify({"error": "Invalid show_id"}), 400

    items = data.get("items") or []
    only_fill_blanks = data.get("only_fill_blanks", True)
    min_confidence = data.get("min_confidence", AI_MIN_CONFIDENCE)
    top_k = data.get("top_k", AI_TOP_K)

    if not items:
        items = []
        for row in iter_show_items_for_ai(show_id, only_with_labels=False):
            items.append(
                {
                    "item_name": row["item_name"],
                    "image": image_path_to_url(normalize_embedding_path(row["image_path"])),
                }
            )

    if not items:
        return jsonify({"error": "No items with images to scan"}), 400

    reference_records, reference_matrix = load_reference_embeddings()
    if not reference_records or reference_matrix is None:
        return jsonify(
            {"error": "No reference embeddings yet. Build the index first."}
        ), 400

    item_names = [item.get("item_name") for item in items if item.get("item_name")]
    existing_map = (
        get_item_fields_map(show_id, item_names) if only_fill_blanks else {}
    )

    results = []
    skipped_no_image = 0
    skipped_existing = 0
    processed = 0
    sku_preset_map = get_sku_preset_map()

    import numpy as np

    for item in items:
        item_name = (item.get("item_name") or "").strip()
        if not item_name:
            continue
        if only_fill_blanks:
            existing = existing_map.get(item_name, {})
            if (existing.get("sku") or "").strip() or (
                existing.get("preset_name") or ""
            ).strip():
                skipped_existing += 1
                continue
        image_ref = item.get("image") or item.get("image_path") or item.get(
            "image_url"
        )
        image_path = resolve_image_path(image_ref, show_id=show_id)
        if not image_path or not image_path.exists():
            skipped_no_image += 1
            continue

        image_key = normalize_embedding_path(image_path)
        rejected_for_source = get_rejected_matches_for_source(image_key)
        stored = fetch_embedding(image_key, show_id)
        if stored:
            embedding_vec = deserialize_embedding(stored[0], stored[1])
        else:
            embedding_vec = compute_image_embedding(image_path)
            if embedding_vec is None:
                skipped_no_image += 1
                continue
            embedding_bytes, embedding_dim = serialize_embedding(embedding_vec)
            upsert_embedding(
                image_key,
                show_id,
                item_name,
                None,
                None,
                embedding_bytes,
                embedding_dim,
            )
        if embedding_vec.shape[0] != reference_matrix.shape[1]:
            continue

        processed += 1
        vec = np.asarray(embedding_vec, dtype="float32")
        vec_norm = np.linalg.norm(vec)
        if vec_norm:
            vec = vec / vec_norm
        scores = reference_matrix @ vec
        if scores.size == 0:
            continue
        sorted_idx = np.argsort(-scores)
        best = None
        best_score = None
        second_score = None
        max_checks = max(2, int(top_k))
        for idx in sorted_idx[:max_checks]:
            score = float(scores[int(idx)])
            if score < float(min_confidence):
                break
            candidate = reference_records[int(idx)]
            if not (candidate.get("sku") or candidate.get("preset_name")):
                continue
            candidate_path = candidate.get("image_path")
            if candidate_path and candidate_path in rejected_for_source:
                continue
            if best is None:
                best = candidate
                best_score = score
            else:
                second_score = score
                break
        if not best:
            continue
        if second_score is not None and best_score is not None:
            if (best_score - second_score) < float(AI_SCORE_GAP):
                continue
        matched_image = (
            image_path_to_url(best.get("image_path"))
            if best.get("image_path")
            else None
        )
        resolved_preset = best.get("preset_name")
        if not resolved_preset and best.get("sku"):
            resolved_preset = sku_preset_map.get(best.get("sku"))
        results.append(
            {
                "item_name": item_name,
                "sku": best.get("sku"),
                "preset_name": resolved_preset,
                "confidence": best_score,
                "matched_item_name": best.get("item_name"),
                "matched_show_id": best.get("show_id"),
                "matched_image": matched_image,
                "source_image": image_path_to_url(image_key),
            }
        )

    return jsonify(
        {
            "results": results,
            "stats": {
                "processed": processed,
                "skipped_no_image": skipped_no_image,
                "skipped_existing": skipped_existing,
            },
        }
    )


@app.route("/api/ai/apply", methods=["POST"])
def ai_apply():
    """Apply AI results to items."""
    data = request.json or {}
    show_id = data.get("show_id")
    results = data.get("results") or []
    only_fill_blanks = data.get("only_fill_blanks", True)
    include_sku = bool(data.get("include_sku", False))

    if not show_id:
        return jsonify({"error": "show_id is required"}), 400
    try:
        show_id = int(show_id)
    except ValueError:
        return jsonify({"error": "Invalid show_id"}), 400

    if not results:
        return jsonify({"error": "No results to apply"}), 400

    item_names = [r.get("item_name") for r in results if r.get("item_name")]
    existing_map = (
        get_item_fields_map(show_id, item_names) if only_fill_blanks else {}
    )
    preset_costs = get_preset_cost_map()
    sku_preset_map = get_sku_preset_map()

    updated = 0
    skipped = 0
    print(f"[AI APPLY] show_id={show_id}, results={len(results)}, only_fill_blanks={only_fill_blanks}, include_sku={include_sku}")
    for result in results:
        item_name = (result.get("item_name") or "").strip()
        if not item_name:
            print(f"[AI APPLY] SKIP: empty item_name")
            skipped += 1
            continue
        if only_fill_blanks:
            existing = existing_map.get(item_name, {})
            if (existing.get("sku") or "").strip() or (
                existing.get("preset_name") or ""
            ).strip():
                print(f"[AI APPLY] SKIP (has preset/sku): {item_name} -> preset={existing.get('preset_name')}, sku={existing.get('sku')}")
                skipped += 1
                continue

        sku = (result.get("sku") or "").strip() or None
        preset_name = (result.get("preset_name") or "").strip() or None
        if not preset_name and sku:
            preset_name = sku_preset_map.get(sku)
        if not (preset_name or (include_sku and sku)):
            print(f"[AI APPLY] SKIP (no preset/sku in result): {item_name}")
            skipped += 1
            continue
        print(f"[AI APPLY] APPLYING: {item_name} -> preset={preset_name}, sku={sku}")

        cost_value = _UNSET
        if preset_name and preset_name in preset_costs:
            cost_value = preset_costs[preset_name]

        set_item_cost(
            item_name,
            cost=cost_value,
            show_id=show_id,
            preset_name=preset_name if preset_name else _UNSET,
            sku=sku if (include_sku and sku) else _UNSET,
        )
        try:
            source_image = result.get("source_image")
            source_path = resolve_image_path(source_image, show_id=show_id)
            if source_path and source_path.exists():
                image_key = normalize_embedding_path(source_path)
                stored = fetch_embedding(image_key, show_id)
                if stored:
                    embedding_bytes, embedding_dim = stored[0], stored[1]
                else:
                    embedding_vec = compute_image_embedding(source_path)
                    if embedding_vec is not None:
                        embedding_bytes, embedding_dim = serialize_embedding(
                            embedding_vec
                        )
                    else:
                        embedding_bytes, embedding_dim = None, None
                if embedding_bytes and embedding_dim:
                    upsert_embedding(
                        image_key,
                        show_id,
                        item_name,
                        sku if (include_sku and sku) else None,
                        preset_name,
                        embedding_bytes,
                        embedding_dim,
                    )
        except Exception:
            pass
        updated += 1

    return jsonify({"updated": updated, "skipped": skipped})


@app.route("/api/ai/feedback", methods=["POST"])
def ai_feedback():
    """Save AI feedback (rejects)."""
    data = request.json or {}
    action = (data.get("action") or "").strip().lower()
    source_image = data.get("source_image")
    matched_image = data.get("matched_image")
    show_id = data.get("show_id")
    org_id = get_current_org_id()

    if action not in {"reject"}:
        return jsonify({"error": "Invalid action"}), 400
    try:
        show_id = int(show_id) if show_id is not None else None
    except ValueError:
        show_id = None

    source_path = resolve_image_path(source_image, show_id=show_id)
    matched_path = resolve_image_path(matched_image, show_id=show_id)
    if not source_path or not matched_path:
        return jsonify({"error": "Invalid image paths"}), 400

    source_key = normalize_embedding_path(source_path)
    matched_key = normalize_embedding_path(matched_path)

    execute(
        """
        INSERT OR IGNORE INTO ai_feedback
        (source_image_path, matched_image_path, action, org_id)
        VALUES (?, ?, ?, ?)
    """,
        (source_key, matched_key, action, org_id),
    )
    return jsonify({"success": True})


@app.route("/api/ai/learn", methods=["POST"])
def ai_learn():
    """Learn embeddings from manual edits."""
    data = request.json or {}
    show_id = data.get("show_id")
    item_name = (data.get("item_name") or "").strip()
    sku = (data.get("sku") or "").strip() or None
    preset_name = (data.get("preset_name") or "").strip() or None
    image_ref = data.get("image")

    if not item_name:
        return jsonify({"error": "Item name required"}), 400
    if show_id is None:
        return jsonify({"error": "show_id required"}), 400
    try:
        show_id = int(show_id)
    except ValueError:
        return jsonify({"error": "Invalid show_id"}), 400
    if not (sku or preset_name):
        return jsonify({"skipped": True})

    image_path = resolve_image_path(image_ref, show_id=show_id)
    if not image_path or not image_path.exists():
        return jsonify({"skipped": True})

    image_key = normalize_embedding_path(image_path)
    stored = fetch_embedding(image_key, show_id)
    if stored:
        embedding_bytes, embedding_dim = stored[0], stored[1]
    else:
        embedding_vec = compute_image_embedding(image_path)
        if embedding_vec is None:
            return jsonify({"skipped": True})
        embedding_bytes, embedding_dim = serialize_embedding(embedding_vec)

    upsert_embedding(
        image_key,
        show_id,
        item_name,
        sku,
        preset_name,
        embedding_bytes,
        embedding_dim,
    )
    return jsonify({"success": True})


@app.route("/api/shows", methods=["GET"])
def get_shows():
    """Get all shows - uses API key org or session org."""
    org_id = get_org_from_api_key_or_session()
    if org_id:
        rows = fetch_all(
            "SELECT id, name, date, created_at FROM shows WHERE org_id = ? ORDER BY created_at DESC",
            (org_id,),
        )
    else:
        rows = fetch_all(
            "SELECT id, name, date, created_at FROM shows ORDER BY created_at DESC",
            (),
        )
    shows = [
        {"id": row[0], "name": row[1], "date": row[2], "created_at": row[3]}
        for row in rows
    ]
    return jsonify(shows)


@app.route("/api/shows", methods=["POST"])
def create_show():
    """Create a new show."""
    data = request.json
    name = data.get("name", "").strip()
    date = data.get("date", "").strip()
    org_id = get_current_org_id()
    
    if not name or not date:
        return jsonify({"error": "Name and date required"}), 400
    
    show_id = execute_returning_id(
        "INSERT INTO shows (org_id, name, date) VALUES (?, ?, ?)",
        (org_id, name, date),
    )

    # Create show-specific folder and CSV file. The DB row is already the
    # source of truth; treating disk writes as best-effort means that on
    # Render (ephemeral disk, may be empty between container restarts) a
    # transient filesystem error doesn't 500 the whole request.
    try:
        show_dir = show_dir_path(name, date)
        show_dir.mkdir(parents=True, exist_ok=True)
        show_csv = show_dir / "log.csv"
        with open(show_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "timestamp",
                    "item_title",
                    "pinned_text",
                    "filename",
                    "sold_price",
                    "sold_timestamp",
                ]
            )
    except Exception as e:
        print(f"[create_show] disk init skipped ({e}); row {show_id} only in DB")

    return jsonify({"id": show_id, "name": name, "date": date})


@app.route("/api/shows/<int:show_id>/duplicate", methods=["POST"])
def duplicate_show(show_id):
    """Duplicate an existing show (CSV + items)."""
    org_id = get_current_org_id()
    with get_db() as (conn, cursor):
        cursor.execute(
            "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
            (show_id, org_id),
        )
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Show not found"}), 404
        original_name, original_date = result

        # Generate a unique name
        base_name = f"{original_name} (Copy)"
        new_name = base_name
        suffix = 2
        cursor.execute(
            "SELECT name FROM shows WHERE date = ? AND org_id = ?",
            (original_date, org_id),
        )
        existing_names = {row[0] for row in cursor.fetchall()}
        while new_name in existing_names:
            new_name = f"{base_name} {suffix}"
            suffix += 1

        if is_postgres():
            cursor.execute(
                "INSERT INTO shows (org_id, name, date) VALUES (%s, %s, %s) RETURNING id",
                (org_id, new_name, original_date),
            )
            new_show_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                "INSERT INTO shows (org_id, name, date) VALUES (?, ?, ?)",
                (org_id, new_name, original_date),
            )
            new_show_id = cursor.lastrowid
        conn.commit()

    # Create show-specific folder and copy files
    original_dir = show_dir_path(original_name, original_date)
    new_dir = show_dir_path(new_name, original_date)
    new_dir.mkdir(parents=True, exist_ok=True)

    if original_dir.exists() and original_dir.is_dir():
        shutil.copytree(original_dir, new_dir, dirs_exist_ok=True)
    else:
        # Create empty CSV if original doesn't exist
        show_csv = new_dir / "log.csv"
        with open(show_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "timestamp",
                    "item_title",
                    "pinned_text",
                    "filename",
                    "sold_price",
                    "sold_timestamp",
                ]
            )

    # Copy items rows in DB.
    # We use plain INSERT (not INSERT OR REPLACE) because new_show_id is freshly
    # minted, so there can be no conflicts on (item_name, new_show_id). This
    # also keeps the statement portable: the SQLite→PG translator doesn't
    # handle INSERT OR REPLACE with an INSERT-SELECT (no VALUES clause).
    execute(
        """
        INSERT INTO items (item_name, show_id, org_id, cost, preset_name, sku, notes, buyer, order_id, cancelled_status)
        SELECT item_name, ?, org_id, cost, preset_name, sku, notes, buyer, order_id, cancelled_status
        FROM items
        WHERE show_id = ? AND org_id = ?
    """,
        (new_show_id, show_id, org_id),
    )

    return jsonify({"id": new_show_id, "name": new_name, "date": original_date})


@app.route("/api/shows/<int:show_id>", methods=["DELETE"])
def delete_show(show_id):
    """Delete a show and its associated data."""
    org_id = get_current_org_id()
    result = fetch_one(
        "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
        (show_id, org_id),
    )
    if not result:
        return jsonify({"error": "Show not found"}), 404
    show_name, show_date = result

    # Delete items tied to the show
    with get_db() as (conn, cursor):
        cursor.execute(
            "DELETE FROM items WHERE show_id = ? AND org_id = ?",
            (show_id, org_id),
        )
        # Delete the show record
        cursor.execute(
            "DELETE FROM shows WHERE id = ? AND org_id = ?",
            (show_id, org_id),
        )
        conn.commit()

    # Remove show folder
    show_dir = show_dir_path(show_name, show_date)
    if show_dir.exists() and show_dir.is_dir():
        shutil.rmtree(show_dir, ignore_errors=True)

    return jsonify({"success": True})


@app.route("/api/shows/<int:show_id>", methods=["PATCH"])
def rename_show(show_id):
    """Rename a show (updates DB and folder name)."""
    data = request.json or {}
    new_name = (data.get("name") or "").strip()
    if not new_name:
        return jsonify({"error": "Name required"}), 400

    org_id = get_current_org_id()
    row = fetch_one(
        "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
        (show_id, org_id),
    )
    if not row:
        return jsonify({"error": "Show not found"}), 404
    old_name, show_date = row

    # Update DB
    execute(
        "UPDATE shows SET name = ? WHERE id = ? AND org_id = ?",
        (new_name, show_id, org_id),
    )

    # Rename folder on disk if it exists
    old_dir = show_dir_path(old_name, show_date)
    new_dir = show_dir_path(new_name, show_date)
    try:
        if old_dir.exists() and old_dir.is_dir() and old_dir != new_dir:
            if new_dir.exists():
                return jsonify({"error": "A show folder with this name already exists"}), 409
            old_dir.rename(new_dir)
    except Exception as exc:
        return jsonify({"error": f"Failed to rename show folder: {exc}"}), 500

    return jsonify({"success": True, "name": new_name})


@app.route("/api/shows/<int:show_id>/export", methods=["GET"])
def export_show(show_id):
    """Export a show as a zip (manifest + log.csv + images).

    Historically this read log.csv and the image files from disk, but on
    Render the disk is ephemeral — every container restart wipes it. So
    the function now builds the zip from the DB when disk files are
    missing: log.csv is generated from the items table, and images come
    out of items.image_data (BYTEA on PG, BLOB on SQLite). On local
    SQLite the disk path still works like before.
    """
    org_id = get_current_org_id()
    result = fetch_one(
        "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
        (show_id, org_id),
    )

    if not result:
        return jsonify({"error": "Show not found"}), 404

    show_name, show_date = result
    show_dir = show_dir_path(show_name, show_date)
    csv_file = show_dir / "log.csv"
    have_disk = show_dir.exists() and csv_file.exists()

    # Pull metadata WITHOUT image_data first. On Render's 512MB starter
    # worker, loading all BYTEA blobs at once can OOM on big shows
    # (464 × 200 KB ≈ 90 MB, plus 2× psycopg2 overhead). Images are
    # streamed one-at-a-time below.
    items_rows = fetch_all(
        """SELECT item_name, cost, preset_name, sku, notes, buyer, order_id,
                  cancelled_status, sold_price, sold_timestamp, viewers, filename,
                  pinned_message
           FROM items WHERE show_id = ? AND org_id = ?
           ORDER BY item_name""",
        (show_id, org_id),
    )

    # Bail only if we have literally nothing — no disk file AND no DB rows.
    if not have_disk and not items_rows:
        return jsonify({"error": "Show data not found"}), 404

    items_data = []
    for row in items_rows:
        items_data.append({
            "item_name": row[0],
            "cost": row[1],
            "preset_name": row[2],
            "sku": row[3],
            "notes": row[4],
            "buyer": row[5],
            "order_id": row[6],
            "cancelled_status": row[7],
        })

    manifest = {
        "name": show_name,
        "date": show_date,
        "exported_at": datetime.now().isoformat(),
        "items": items_data,
    }

    import io, csv as _csv
    from PIL import Image as PILImage
    print(f"[EXPORT] show={show_name} dir_exists={have_disk} items={len(items_rows)}")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))

        # ---- log.csv ----
        if have_disk:
            zf.write(csv_file, arcname="log.csv")
        else:
            # Build an equivalent log.csv in memory from the items table.
            csv_buf = io.StringIO()
            writer = _csv.writer(csv_buf)
            writer.writerow([
                "timestamp", "item_title", "pinned_text", "filename",
                "sold_price", "sold_timestamp",
            ])
            for row in items_rows:
                item_name, _cost, _preset, _sku, _notes, _buyer, _order_id, \
                _cancelled, sold_price, sold_ts, _viewers, filename, \
                pinned_message = row
                writer.writerow([
                    sold_ts or "",
                    item_name,
                    pinned_message or "",
                    filename or "",
                    sold_price or "",
                    sold_ts or "",
                ])
            zf.writestr("log.csv", csv_buf.getvalue())

        # ---- images ----
        if have_disk:
            # Disk path — recompress PNGs to JPEG like before.
            for path in show_dir.iterdir():
                if not path.is_file() or path.name == "log.csv":
                    continue
                if path.suffix.lower() == ".png":
                    try:
                        img = PILImage.open(path).convert("RGB")
                        jpg_buffer = io.BytesIO()
                        img.save(jpg_buffer, format="JPEG", quality=75, optimize=True)
                        zf.writestr(f"images/{path.stem}.jpg", jpg_buffer.getvalue())
                    except Exception as e:
                        print(f"[EXPORT] JPEG conversion failed for {path.name}: {e}")
                        zf.write(path, arcname=f"images/{path.name}")
                else:
                    zf.write(path, arcname=f"images/{path.name}")
        else:
            # DB-only path (Render). Fetch image_data one item at a time so
            # we don't blow through the 512MB starter-plan worker memory on
            # big shows (464 items * 200KB = 90 MB just for raw bytes, more
            # after PIL decompression).
            for row in items_rows:
                item_name = row[0]
                filename = row[11]
                img_row = fetch_one(
                    "SELECT image_data FROM items WHERE show_id = ? AND item_name = ?",
                    (show_id, item_name),
                )
                if not img_row or not img_row[0]:
                    continue
                image_bytes = img_row[0]
                if isinstance(image_bytes, memoryview):
                    image_bytes = bytes(image_bytes)
                stored_name = filename or item_name
                stem = stored_name.rsplit(".", 1)[0] if "." in stored_name else stored_name
                safe_stem = safe_filename(stem) or f"item_{item_name}"
                try:
                    img = PILImage.open(io.BytesIO(image_bytes)).convert("RGB")
                    jpg_buffer = io.BytesIO()
                    img.save(jpg_buffer, format="JPEG", quality=75, optimize=True)
                    zf.writestr(f"images/{safe_stem}.jpg", jpg_buffer.getvalue())
                    # Release references so Python/PIL can GC each iteration.
                    del img, jpg_buffer
                except Exception as e:
                    print(f"[EXPORT] raw-fallback for {safe_stem}: {e}")
                    zf.writestr(f"images/{safe_stem}.bin", image_bytes)
                del image_bytes

    buffer.seek(0)
    filename = f"{safe_filename(show_name)}_{safe_filename(show_date)}_show.zip"
    return send_file(
        buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/api/shows/import", methods=["POST"])
def import_show():
    """Import a show from a zip (log.csv + images + items metadata)."""
    org_id = get_current_org_id()
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if not file.filename.endswith(".zip"):
        return jsonify({"error": "File must be a .zip"}), 400

    import io

    raw = file.read()
    if isinstance(raw, bytes):
        data = raw
    else:
        data = bytes(raw)

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            manifest = {}
            if "manifest.json" in zf.namelist():
                manifest = json.loads(zf.read("manifest.json").decode("utf-8"))

            original_name = manifest.get("name") or Path(file.filename).stem
            original_date = manifest.get("date") or datetime.now().strftime("%Y-%m-%d")

            # Create a unique imported show name
            base_name = f"{original_name} (Imported)"
            new_name = base_name
            suffix = 2
            existing_names = {row[0] for row in fetch_all(
                "SELECT name FROM shows WHERE date = ? AND org_id = ?",
                (original_date, org_id),
            )}
            while new_name in existing_names:
                new_name = f"{base_name} {suffix}"
                suffix += 1

            new_show_id = execute_returning_id(
                "INSERT INTO shows (org_id, name, date) VALUES (?, ?, ?)",
                (org_id, new_name, original_date),
            )

            show_dir = show_dir_path(new_name, original_date)
            show_dir.mkdir(parents=True, exist_ok=True)

            # Safe extraction helper
            def safe_member(name: str) -> bool:
                if not name or name.startswith("/") or name.startswith("\\"):
                    return False
                if ".." in Path(name).parts:
                    return False
                return True

            # Extract log.csv
            if "log.csv" in zf.namelist():
                log_bytes = zf.read("log.csv")
                with open(show_dir / "log.csv", "wb") as f:
                    f.write(log_bytes)
            else:
                # Create empty log.csv if missing
                with open(show_dir / "log.csv", "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        [
                            "timestamp",
                            "item_title",
                            "pinned_text",
                            "filename",
                            "sold_price",
                            "sold_timestamp",
                            "viewers",
                        ]
                    )

            # Extract images
            for name in zf.namelist():
                if not safe_member(name):
                    continue
                if not name.startswith("images/"):
                    continue
                filename = Path(name).name
                if not filename:
                    continue
                target = show_dir / filename
                with open(target, "wb") as f:
                    f.write(zf.read(name))

            # Import items metadata
            items_data = manifest.get("items") or []
            for item in items_data:
                item_name = (item.get("item_name") or "").strip()
                if not item_name:
                    continue
                set_item_cost(
                    item_name,
                    item.get("cost", _UNSET),
                    new_show_id,
                    preset_name=item.get("preset_name", _UNSET),
                    sku=item.get("sku", _UNSET),
                    notes=item.get("notes", _UNSET),
                    buyer=item.get("buyer", _UNSET),
                    order_id=item.get("order_id", _UNSET),
                    cancelled_status=item.get("cancelled_status", _UNSET),
                )

            return jsonify(
                {
                    "success": True,
                    "id": new_show_id,
                    "name": new_name,
                    "date": original_date,
                }
            )
    except zipfile.BadZipFile:
        return jsonify({"error": "Invalid zip file"}), 400
    except Exception as e:
        return jsonify({"error": f"Error importing show: {str(e)}"}), 500


@app.route("/api/current-show", methods=["GET"])
def get_current_show():
    """Get the current active show (most recent)."""
    org_id = get_current_org_id()
    result = fetch_one(
        "SELECT id, name, date FROM shows WHERE org_id = ? ORDER BY created_at DESC LIMIT 1",
        (org_id,),
    )
    if result:
        return jsonify({"id": result[0], "name": result[1], "date": result[2]})
    return jsonify(None)


IS_WINDOWS = platform.system() == "Windows"


def kill_processes_by_name(pattern: str):
    """Kill processes matching a pattern (cross-platform using psutil)."""
    killed = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = " ".join(proc.info['cmdline'] or [])
            if pattern in cmdline:
                proc.kill()
                killed += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return killed


def find_monitor_processes():
    """Find running tt_monitor.py processes (cross-platform)."""
    found = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = " ".join(proc.info['cmdline'] or [])
            if "tt_monitor.py" in cmdline and ("python" in cmdline.lower()):
                found.append({"pid": proc.pid, "cmdline": cmdline})
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return found


def spawn_detached_process(cmd, stdout_file, stderr_file, cwd):
    """Spawn a detached subprocess (cross-platform)."""
    if IS_WINDOWS:
        # On Windows, use CREATE_NEW_PROCESS_GROUP + DETACHED_PROCESS
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        DETACHED_PROCESS = 0x00000008
        process = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=stdout_file,
            stderr=stderr_file,
            creationflags=CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS,
            close_fds=True,
        )
    else:
        # On macOS/Linux, use start_new_session
        process = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=stdout_file,
            stderr=stderr_file,
            start_new_session=True,
            close_fds=True,
        )
    return process


@app.route("/api/start-recording", methods=["POST"])
def start_recording():
    """Start recording (launch tt_monitor.py via CDP)."""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Invalid request data"}), 400

        show_id = data.get("show_id")
        org_id = get_current_org_id()

        if not show_id:
            return jsonify({"error": "Show ID required"}), 400

        # Get show name and date from database
        result = fetch_one(
            "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
            (show_id, org_id),
        )

        if not result:
            return jsonify({"error": "Show not found"}), 404

        show_name, show_date = result

        # Kill any existing monitors first
        print("Killing any existing monitor processes...")
        kill_processes_by_name("tt_monitor.py")
        time.sleep(2)

        # Verify they're all dead
        remaining = find_monitor_processes()
        if remaining:
            print(f"Warning: {len(remaining)} monitor processes still running, force killing...")
            kill_processes_by_name("tt_monitor.py")
            time.sleep(1)

        # Launch tt_monitor.py in background
        script_dir = Path(__file__).parent
        monitor_script = script_dir / "tt_monitor.py"

        if not monitor_script.exists():
            return jsonify({"error": "tt_monitor.py not found"}), 404

        log_dir = script_dir / "captures"
        log_dir.mkdir(exist_ok=True)
        stdout_log = log_dir / f"monitor_{show_id}.log"
        stderr_log = log_dir / f"monitor_{show_id}.err"

        # Open log files for writing
        stdout_file = open(stdout_log, "a")
        stderr_file = open(stderr_log, "a")

        # Build command with show info (no URL needed - connects via CDP)
        cmd = [
            sys.executable,
            str(monitor_script),
            "--show-id",
            str(show_id),
            "--show-name",
            show_name,
            "--show-date",
            show_date,
        ]

        # Start detached process (cross-platform)
        process = spawn_detached_process(cmd, stdout_file, stderr_file, script_dir)

        # Close file handles in parent process (child keeps them open)
        stdout_file.close()
        stderr_file.close()

        # Wait a moment to verify process started successfully
        time.sleep(3)
        if process.poll() is not None:
            # Process already exited - read error log
            error_details = "Unknown error"
            try:
                if stderr_log.exists():
                    with open(stderr_log, "r") as f:
                        error_text = f.read()
                        if error_text:
                            error_lines = error_text.split("\n")
                            for line in reversed(error_lines):
                                if (
                                    "Error:" in line
                                    or "Failed" in line
                                    or "CDP" in line
                                ):
                                    error_details = line.strip()
                                    break
                            if not error_details or error_details == "Unknown error":
                                error_details = error_text[-500:]
            except Exception as e:
                error_details = f"Error reading log: {str(e)}"

            return (
                jsonify(
                    {
                        "error": f"Recording failed to start: {error_details}",
                        "suggestion": "Make sure Chrome is running with --remote-debugging-port=9222 and TikTok Shop streamer tab is open.",
                    }
                ),
                500,
            )

        # Verify process is still running
        try:
            check_result = subprocess.run(
                ["ps", "-p", str(process.pid)], capture_output=True, text=True
            )
            if check_result.returncode != 0:
                return (
                    jsonify(
                        {
                            "error": "Recording process started but exited immediately",
                            "suggestion": "Make sure Chrome is running with --remote-debugging-port=9222",
                        }
                    ),
                    500,
                )
        except:
            pass

        # Record the start time for this show
        execute(
            "INSERT INTO recording_sessions (show_id, org_id, started_at) VALUES (?, ?, ?)",
            (show_id, org_id, datetime.now().isoformat()),
        )

        return jsonify(
            {
                "success": True,
                "message": "Recording started. Connected to TikTok Shop tab via CDP.",
                "process_id": process.pid,
            }
        )
    except Exception as e:
        import traceback

        error_msg = str(e)
        traceback.print_exc()
        return jsonify({"error": f"Failed to start recording: {error_msg}"}), 500


@app.route("/api/stop-recording", methods=["POST"])
def stop_recording():
    """Stop the recording process."""
    try:
        # Find and kill monitor processes (cross-platform)
        monitors = find_monitor_processes()
        killed_count = kill_processes_by_name("tt_monitor.py")

        # Wait a moment for processes to die
        time.sleep(1)

        # Close the most recent open recording session
        org_id = get_current_org_id()
        with get_db() as (conn, cursor):
            cursor.execute(
                """
                SELECT id FROM recording_sessions
                WHERE stopped_at IS NULL AND org_id = ?
                ORDER BY started_at DESC
                LIMIT 1
            """,
                (org_id,),
            )
            db_session = cursor.fetchone()
            if db_session:
                cursor.execute(
                    "UPDATE recording_sessions SET stopped_at = ? WHERE id = ? AND org_id = ?",
                    (datetime.now().isoformat(), db_session[0], org_id),
                )
                conn.commit()

        if killed_count > 0:
            return jsonify(
                {
                    "success": True,
                    "message": f"Recording stopped ({killed_count} monitor(s) killed)",
                }
            )
        else:
            return jsonify({"success": False, "message": "No recording process found"})
    except Exception as e:
        return jsonify({"error": f"Failed to stop recording: {str(e)}"}), 500


@app.route("/api/recording-status", methods=["GET"])
def get_recording_status():
    """Check if tt_monitor.py is currently running (cross-platform)."""
    try:
        monitors = find_monitor_processes()
        is_recording = len(monitors) > 0
        process_count = len(monitors)
        process_info = {"pid": monitors[0]["pid"]} if monitors else None

        message = "Recording in progress" if is_recording else "Not recording"
        if process_count > 1:
            message += f" (WARNING: {process_count} monitors running - this may cause conflicts!)"

        return jsonify(
            {
                "is_recording": is_recording,
                "process_info": process_info,
                "process_count": process_count,
                "message": message,
            }
        )
    except Exception as e:
        return jsonify(
            {
                "is_recording": False,
                "process_info": None,
                "message": f"Error checking status: {str(e)}",
            }
        )


# ─── Extension API Endpoints ────────────────────────────────────

@app.route("/api/extension-capture", methods=["POST"])
def extension_capture():
    """Receive a capture from the Chrome extension."""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data received"}), 400

        show_id = data.get("show_id")
        item_title = data.get("item_title", "")
        image_base64 = data.get("image_base64", "")
        timestamp = data.get("timestamp", datetime.now().isoformat())

        if not show_id:
            return jsonify({"error": "show_id required"}), 400
        if not item_title:
            return jsonify({"error": "item_title required"}), 400

        # Look up show - use API key org if provided, otherwise any org
        org_id = get_org_from_api_key_or_session()
        if org_id:
            result = fetch_one(
                "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
                (show_id, org_id),
            )
        else:
            result = fetch_one(
                "SELECT name, date FROM shows WHERE id = ?",
                (show_id,),
            )
        if not result:
            return jsonify({"error": f"Show {show_id} not found"}), 404

        show_name, show_date = result
        captures_dir = get_captures_dir()
        safe_name = re.sub(r'[<>:"/\\|?*]', "", show_name).strip()
        safe_date = re.sub(r'[<>:"/\\|?*]', "", show_date).strip()
        show_dir = captures_dir / f"{safe_name}_{safe_date}"
        show_dir.mkdir(parents=True, exist_ok=True)

        # Save screenshot if provided
        filename = ""
        if image_base64:
            import base64
            sanitized_title = re.sub(r'[<>:"/\\|?*]', "", item_title).replace(" ", "_")
            if len(sanitized_title) > 100:
                sanitized_title = sanitized_title[:100]
            date_str = datetime.now().strftime("%m-%d")
            # Count existing files to create unique counter
            existing = list(show_dir.glob(f"{sanitized_title}*.png"))
            counter = len(existing) + 1
            filename = f"{sanitized_title} {date_str}_{counter:03d}.png"
            filepath = show_dir / filename

            image_bytes = base64.b64decode(image_base64)
            with open(filepath, "wb") as f:
                f.write(image_bytes)

        # Append to CSV (local disk - ephemeral on Render)
        log_file = show_dir / "log.csv"
        write_header = not log_file.exists()
        with open(log_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(["timestamp", "item_title", "pinned_text", "filename",
                                "sold_price", "sold_timestamp", "viewers"])
            writer.writerow([timestamp, item_title, "", filename, "", "", ""])

        # Also persist to items DB table (survives Render restarts)
        # Store image bytes directly in DB too (Render disk is ephemeral)
        insert_org_id = get_org_from_api_key_or_session()
        if not insert_org_id:
            show_org = fetch_one("SELECT org_id FROM shows WHERE id = ?", (show_id,))
            insert_org_id = show_org[0] if show_org else 1

        image_bytes_for_db = None
        if image_base64:
            import base64
            try:
                image_bytes_for_db = base64.b64decode(image_base64)
            except Exception:
                image_bytes_for_db = None

        try:
            if is_postgres():
                execute(
                    """INSERT INTO items (item_name, show_id, org_id, filename, image_data)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT (item_name, show_id) DO UPDATE SET
                         filename = EXCLUDED.filename,
                         image_data = EXCLUDED.image_data""",
                    (item_title, show_id, insert_org_id, filename, image_bytes_for_db),
                )
            else:
                # Upsert image+filename without disturbing preset/cost/sku/etc.
                # INSERT OR REPLACE would wipe every other column on a re-capture.
                execute(
                    """INSERT INTO items (item_name, show_id, org_id, filename, image_data)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT (item_name, show_id) DO UPDATE SET
                         org_id = EXCLUDED.org_id,
                         filename = EXCLUDED.filename,
                         image_data = EXCLUDED.image_data""",
                    (item_title, show_id, insert_org_id, filename, image_bytes_for_db),
                )
        except Exception as db_err:
            import traceback
            traceback.print_exc()
            print(f"[extension-capture] DB insert failed: {db_err}")

        return jsonify({"success": True, "filename": filename, "item_title": item_title})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/extension-sold", methods=["POST"])
def extension_sold():
    """Receive a 'won auction item' notification scraped from the TikTok
    Live chat's 'Top customers' panel. Updates buyer + sold_price on the
    matching item in the current show.

    Body JSON:
      show_id     — int
      item_title  — string (chat form, e.g. "2 Sneaker Pull ... 4/23 A")
      buyer       — string (TikTok display name)
      sold_price  — number or string

    Matching: the chat shows "2 Sneaker Pull..." but the dashboard
    stores "#2 Sneaker Pull...". We try both variants.
    """
    try:
        data = request.get_json(silent=True) or {}
        show_id = data.get("show_id")
        item_title = (data.get("item_title") or "").strip()
        buyer = (data.get("buyer") or "").strip()
        raw_price = data.get("sold_price")
        if not show_id or not item_title:
            return jsonify({"success": False, "error": "show_id + item_title required"}), 400
        try:
            show_id = int(show_id)
        except (TypeError, ValueError):
            return jsonify({"success": False, "error": "bad show_id"}), 400

        # Normalize price
        try:
            sold_price = float(raw_price) if raw_price not in (None, "") else None
        except (TypeError, ValueError):
            sold_price = None
        sold_price_str = f"{sold_price:.2f}" if sold_price is not None else str(raw_price or "")

        org_id = get_org_from_api_key_or_session()

        # Try a few variants to bridge "2 Foo" (chat) vs "#2 Foo" (dashboard).
        variants = [item_title]
        if not item_title.startswith("#"):
            variants.append("#" + item_title)
        # Also tolerate accidental leading whitespace/NBSP on either side.
        extra = []
        for v in variants:
            if v.replace("#", "").strip() != v:
                extra.append(v.strip())
        variants += extra

        # Match in the show.
        placeholders = ",".join(["?"] * len(variants))
        if org_id:
            row = fetch_one(
                f"SELECT item_name FROM items "
                f"WHERE show_id = ? AND org_id = ? AND item_name IN ({placeholders})",
                (show_id, org_id, *variants),
            )
        else:
            row = fetch_one(
                f"SELECT item_name FROM items "
                f"WHERE show_id = ? AND item_name IN ({placeholders})",
                (show_id, *variants),
            )
        if not row:
            return jsonify({"success": False, "error": "item not found", "tried": variants}), 404
        matched_name = row[0]

        # Update buyer + sold_price. Non-destructive — only overwrites
        # these two columns; everything else (preset, cost, image_data, ...)
        # stays intact.
        with get_db() as (conn, cursor):
            cursor.execute(
                "UPDATE items SET buyer = ?, sold_price = ? "
                "WHERE item_name = ? AND show_id = ?",
                (buyer, sold_price_str, matched_name, show_id),
            )
            conn.commit()

        return jsonify({
            "success": True,
            "item_name": matched_name,
            "buyer": buyer,
            "sold_price": sold_price_str,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/extension-start", methods=["POST"])
def extension_start():
    """Called by extension when recording starts (creates recording session)."""
    try:
        data = request.json or {}
        show_id = data.get("show_id")
        org_id = get_org_from_api_key_or_session()
        if show_id:
            execute(
                "INSERT INTO recording_sessions (show_id, org_id, started_at) VALUES (?, ?, ?)",
                (show_id, org_id, datetime.now().isoformat()),
            )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/extension-stop", methods=["POST"])
def extension_stop():
    """Called by extension when recording stops."""
    try:
        org_id = get_org_from_api_key_or_session()
        with get_db() as (conn, cursor):
            cursor.execute(
                """
                SELECT id FROM recording_sessions
                WHERE stopped_at IS NULL AND org_id = ?
                ORDER BY started_at DESC
                LIMIT 1
            """,
                (org_id,),
            )
            db_session = cursor.fetchone()
            if db_session:
                cursor.execute(
                    "UPDATE recording_sessions SET stopped_at = ? WHERE id = ? AND org_id = ?",
                    (datetime.now().isoformat(), db_session[0], org_id),
                )
                conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/api-key", methods=["GET"])
def get_api_key():
    """Get the API key for the current org (owner only)."""
    org_id = get_current_org_id()
    if not org_id:
        return jsonify({"error": "Not logged in"}), 401
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not logged in"}), 401
    row = fetch_one("SELECT role FROM memberships WHERE user_id = ? AND org_id = ?", (user["id"], org_id))
    if not row or row[0] != "owner":
        return jsonify({"error": "Owner only"}), 403
    key_row = fetch_one("SELECT api_key FROM orgs WHERE id = ?", (org_id,))
    return jsonify({"api_key": key_row[0] if key_row else None})


@app.route("/api/presets", methods=["GET"])
def get_presets():
    """Get all cost presets."""
    org_id = get_current_org_id()
    rows = fetch_all(
        """
        SELECT
            presets.id,
            presets.name,
            presets.cost,
            presets.is_giveaway,
            preset_groups.id,
            preset_groups.name
        FROM presets
        LEFT JOIN preset_group_links
            ON preset_group_links.preset_id = presets.id
            AND preset_group_links.org_id = presets.org_id
        LEFT JOIN preset_groups
            ON preset_groups.id = preset_group_links.group_id
            AND preset_groups.org_id = presets.org_id
        WHERE presets.org_id = ?
        ORDER BY presets.name, preset_groups.name
    """,
        (org_id,),
    )
    presets_map = {}
    for row in rows:
        preset_id, name, cost, is_giveaway, group_id, group_name = row
        if preset_id not in presets_map:
            presets_map[preset_id] = {
                "id": preset_id,
                "name": name,
                "cost": cost,
                "is_giveaway": bool(is_giveaway),
                "groups": [],
            }
        if group_id:
            presets_map[preset_id]["groups"].append(
                {"id": group_id, "name": group_name}
            )
    presets = list(presets_map.values())
    return jsonify(presets)


@app.route("/api/presets", methods=["POST"])
def create_preset():
    """Create a new cost preset."""
    data = request.json
    name = data.get("name", "").strip()
    cost = data.get("cost")
    is_giveaway = bool(data.get("is_giveaway", False))
    group_ids = data.get("group_ids")
    group_id = data.get("group_id")
    org_id = get_current_org_id()
    
    if not name:
        return jsonify({"error": "Preset name required"}), 400
    
    try:
        cost_float = float(cost)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid cost"}), 400

    if group_ids is None and group_id is not None:
        group_ids = [group_id]
    if group_ids is None:
        group_ids = []
    cleaned_group_ids = []
    for group in group_ids:
        if group in ("", None):
            continue
        try:
            cleaned_group_ids.append(int(group))
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid group"}), 400
    
    try:
        with get_db() as (conn, cursor):
            if is_postgres():
                cursor.execute(
                    "INSERT INTO presets (org_id, name, cost, is_giveaway) VALUES (%s, %s, %s, %s) RETURNING id",
                    (org_id, name, cost_float, 1 if is_giveaway else 0),
                )
                preset_id = cursor.fetchone()[0]
            else:
                cursor.execute(
                    "INSERT INTO presets (org_id, name, cost, is_giveaway) VALUES (?, ?, ?, ?)",
                    (org_id, name, cost_float, 1 if is_giveaway else 0),
                )
                preset_id = cursor.lastrowid
            for group_id in cleaned_group_ids:
                cursor.execute(
                    "INSERT OR IGNORE INTO preset_group_links (preset_id, group_id, org_id) VALUES (?, ?, ?)",
                    (preset_id, group_id, org_id),
                )
            conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower() or "UNIQUE" in str(e):
            return jsonify({"error": "Preset already exists"}), 400
        raise


@app.route("/api/presets/<name>", methods=["DELETE"])
def delete_preset(name):
    """Delete a cost preset."""
    org_id = get_current_org_id()
    execute("DELETE FROM presets WHERE name = ? AND org_id = ?", (name, org_id))
    return jsonify({"success": True})


@app.route("/api/presets/<int:preset_id>", methods=["PATCH"])
def update_preset(preset_id):
    """Update preset name, cost, or group."""
    org_id = get_current_org_id()
    data = request.json or {}
    name = data.get("name")
    cost = data.get("cost")
    is_giveaway = data.get("is_giveaway", _UNSET)
    group_ids = data.get("group_ids", _UNSET)

    if name is not None:
        name = name.strip()
        if not name:
            return jsonify({"error": "Preset name required"}), 400

    cost_float = _UNSET
    if cost is not None:
        try:
            cost_float = float(cost)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid cost"}), 400

    cleaned_group_ids = _UNSET
    if group_ids is not _UNSET:
        if group_ids is None:
            group_ids = []
        cleaned_group_ids = []
        for group in group_ids:
            if group in ("", None):
                continue
            try:
                cleaned_group_ids.append(int(group))
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid group"}), 400

    updates = []
    params = []
    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if cost_float is not _UNSET:
        updates.append("cost = ?")
        params.append(cost_float)
    if is_giveaway is not _UNSET:
        updates.append("is_giveaway = ?")
        params.append(1 if bool(is_giveaway) else 0)
    if not updates and cleaned_group_ids is _UNSET:
        return jsonify({"error": "No updates provided"}), 400

    try:
        with get_db() as (conn, cursor):
            if updates:
                params.append(preset_id)
                cursor.execute(
                    f"UPDATE presets SET {', '.join(updates)} WHERE id = ? AND org_id = ?",
                    params + [org_id],
                )
                if cursor.rowcount == 0:
                    return jsonify({"error": "Preset not found"}), 404
            if cleaned_group_ids is not _UNSET:
                cursor.execute(
                    "DELETE FROM preset_group_links WHERE preset_id = ? AND org_id = ?",
                    (preset_id, org_id),
                )
                for group_id in cleaned_group_ids:
                    cursor.execute(
                        "INSERT OR IGNORE INTO preset_group_links (preset_id, group_id, org_id) VALUES (?, ?, ?)",
                        (preset_id, group_id, org_id),
                    )
            conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            return jsonify({"error": "Preset name already exists"}), 400
        raise


@app.route("/api/presets/<int:preset_id>", methods=["DELETE"])
def delete_preset_by_id(preset_id):
    """Delete a preset by id."""
    org_id = get_current_org_id()
    with get_db() as (conn, cursor):
        cursor.execute(
            "DELETE FROM preset_group_links WHERE preset_id = ? AND org_id = ?",
            (preset_id, org_id),
        )
        cursor.execute(translate("DELETE FROM presets WHERE id = ? AND org_id = ?"), (preset_id, org_id))
        conn.commit()
    return jsonify({"success": True})


@app.route("/api/preset-groups", methods=["GET"])
def get_preset_groups():
    """Get all preset groups with counts."""
    org_id = get_current_org_id()
    rows = fetch_all(
        """
        SELECT preset_groups.id, preset_groups.name, COUNT(presets.id)
        FROM preset_groups
        LEFT JOIN preset_group_links
            ON preset_group_links.group_id = preset_groups.id
            AND preset_group_links.org_id = preset_groups.org_id
        LEFT JOIN presets
            ON presets.id = preset_group_links.preset_id
            AND presets.org_id = preset_group_links.org_id
        WHERE preset_groups.org_id = ?
        GROUP BY preset_groups.id, preset_groups.name
        ORDER BY preset_groups.name
    """,
        (org_id,),
    )
    groups = [
        {"id": row[0], "name": row[1], "preset_count": row[2]}
        for row in rows
    ]
    return jsonify(groups)


@app.route("/api/preset-groups", methods=["POST"])
def create_preset_group():
    """Create a preset group."""
    data = request.json
    name = data.get("name", "").strip()
    org_id = get_current_org_id()

    if not name:
        return jsonify({"error": "Group name required"}), 400

    try:
        execute("INSERT INTO preset_groups (org_id, name) VALUES (?, ?)", (org_id, name))
        return jsonify({"success": True})
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            return jsonify({"error": "Group already exists"}), 400
        raise


@app.route("/api/preset-groups/<int:group_id>", methods=["PATCH"])
def update_preset_group(group_id):
    """Rename a preset group."""
    org_id = get_current_org_id()
    data = request.json or {}
    name = data.get("name", "").strip()

    if not name:
        return jsonify({"error": "Group name required"}), 400

    try:
        with get_db() as (conn, cursor):
            cursor.execute(
                "UPDATE preset_groups SET name = ? WHERE id = ? AND org_id = ?",
                (name, group_id, org_id),
            )
            if cursor.rowcount == 0:
                return jsonify({"error": "Group not found"}), 404
            conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            return jsonify({"error": "Group already exists"}), 400
        raise


@app.route("/api/preset-groups/<int:group_id>", methods=["DELETE"])
def delete_preset_group(group_id):
    """Delete a preset group and ungroup presets."""
    org_id = get_current_org_id()
    with get_db() as (conn, cursor):
        cursor.execute(
            "DELETE FROM preset_group_links WHERE group_id = ? AND org_id = ?",
            (group_id, org_id),
        )
        cursor.execute(
            "DELETE FROM preset_groups WHERE id = ? AND org_id = ?",
            (group_id, org_id),
        )
        conn.commit()
    return jsonify({"success": True})


@app.route("/api/skus", methods=["GET"])
def get_skus():
    """Get all SKUs with usage counts and preset mapping."""
    org_id = get_current_org_id()
    with get_db() as (conn, cursor):
        cursor.execute(
            """
            SELECT sku, COUNT(*) as use_count
            FROM items
            WHERE sku IS NOT NULL AND TRIM(sku) != '' AND org_id = ?
            GROUP BY sku
            ORDER BY use_count DESC
        """,
            (org_id,),
        )
        sku_counts = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute(
            """
            SELECT sku, preset_name, COUNT(*) as cnt
            FROM items
            WHERE sku IS NOT NULL AND TRIM(sku) != ''
              AND preset_name IS NOT NULL AND TRIM(preset_name) != ''
              AND org_id = ?
            GROUP BY sku, preset_name
            ORDER BY cnt DESC
        """,
            (org_id,),
        )
        sku_preset_counts = cursor.fetchall()

        cursor.execute(translate("SELECT sku, preset_name FROM sku_presets WHERE org_id = ?"), (org_id,))
        sku_presets = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute(translate("SELECT name, cost FROM presets WHERE org_id = ? ORDER BY name"), (org_id,))
        presets = [{"name": row[0], "cost": row[1]} for row in cursor.fetchall()]

    # Backfill sku_presets from item history (most common preset per SKU)
    if sku_preset_counts:
        best_map = {}
        for sku, preset_name, cnt in sku_preset_counts:
            if sku not in best_map:
                best_map[sku] = preset_name
        for sku, preset_name in best_map.items():
            if sku not in sku_presets and preset_name:
                upsert_sku_preset(sku, preset_name)
                sku_presets[sku] = preset_name

    # Union SKUs from items + saved mappings
    sku_set = set(sku_counts.keys()) | set(sku_presets.keys())
    skus = []
    for sku in sorted(sku_set, key=lambda s: (s or "").lower()):
        preset_name = sku_presets.get(sku) or ""
        skus.append(
            {
                "sku": sku,
                "preset_name": preset_name,
                "use_count": sku_counts.get(sku, 0),
            }
        )

    total_skus = len(skus)
    mapped_skus = sum(1 for s in skus if s["preset_name"])
    unmapped_skus = total_skus - mapped_skus

    preset_usage = {}
    for sku, preset_name in sku_presets.items():
        if not preset_name:
            continue
        preset_usage[preset_name] = preset_usage.get(preset_name, 0) + 1
    top_presets = sorted(
        [{"name": name, "sku_count": count} for name, count in preset_usage.items()],
        key=lambda r: r["sku_count"],
        reverse=True,
    )[:5]

    return jsonify(
        {
            "skus": skus,
            "presets": presets,
            "insights": {
                "total_skus": total_skus,
                "mapped_skus": mapped_skus,
                "unmapped_skus": unmapped_skus,
                "top_presets": top_presets,
            },
        }
    )


@app.route("/api/skus/<path:sku>", methods=["PATCH"])
def update_sku_mapping(sku):
    """Update preset mapping for a SKU."""
    data = request.json or {}
    preset_name = (data.get("preset_name") or "").strip()
    sku_clean = (sku or "").strip()
    org_id = get_current_org_id()
    if not sku_clean:
        return jsonify({"error": "SKU required"}), 400

    if not preset_name:
        # Clear mapping
        execute(
            "DELETE FROM sku_presets WHERE sku = ? AND org_id = ?",
            (sku_clean, org_id),
        )
        return jsonify({"success": True})

    upsert_sku_preset(sku_clean, preset_name)
    return jsonify({"success": True})


@app.route("/api/skus/<path:sku>", methods=["DELETE"])
def delete_sku(sku):
    """Delete a SKU from items and saved mappings."""
    sku_clean = (sku or "").strip()
    org_id = get_current_org_id()
    if not sku_clean:
        return jsonify({"error": "SKU required"}), 400

    with get_db() as (conn, cursor):
        cursor.execute(
            "UPDATE items SET sku = NULL WHERE sku = ? AND org_id = ?",
            (sku_clean, org_id),
        )
        cursor.execute(
            "DELETE FROM sku_presets WHERE sku = ? AND org_id = ?",
            (sku_clean, org_id),
        )
        conn.commit()
    return jsonify({"success": True})


@app.route("/api/skus/<path:sku>", methods=["PUT"])
def rename_sku(sku):
    """Rename a SKU across items and saved mappings."""
    sku_clean = (sku or "").strip()
    data = request.json or {}
    new_sku = (data.get("new_sku") or "").strip()
    org_id = get_current_org_id()
    if not sku_clean:
        return jsonify({"error": "SKU required"}), 400
    if not new_sku:
        return jsonify({"error": "New SKU required"}), 400

    with get_db() as (conn, cursor):
        cursor.execute(
            "UPDATE items SET sku = ? WHERE sku = ? AND org_id = ?",
            (new_sku, sku_clean, org_id),
        )
        cursor.execute(
            "UPDATE sku_presets SET sku = ? WHERE sku = ? AND org_id = ?",
            (new_sku, sku_clean, org_id),
        )
        conn.commit()
    return jsonify({"success": True})


@app.route("/api/presets/insights", methods=["GET"])
def get_preset_insights():
    """Get quick insights about presets."""
    org_id = get_current_org_id()
    with get_db() as (conn, cursor):
        cursor.execute(translate("SELECT COUNT(*) FROM presets WHERE org_id = ?"), (org_id,))
        total_presets = cursor.fetchone()[0]

        cursor.execute(translate("SELECT COUNT(*) FROM preset_groups WHERE org_id = ?"), (org_id,))
        total_groups = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*) FROM presets
            LEFT JOIN preset_group_links
                ON preset_group_links.preset_id = presets.id
                AND preset_group_links.org_id = ?
            WHERE preset_group_links.preset_id IS NULL
              AND presets.org_id = ?
        """,
            (org_id, org_id),
        )
        ungrouped = cursor.fetchone()[0]

        cursor.execute(
            "SELECT AVG(cost), MIN(cost), MAX(cost) FROM presets WHERE org_id = ?",
            (org_id,),
        )
        avg_cost, min_cost, max_cost = cursor.fetchone()

        cursor.execute(
            """
            SELECT preset_name, COUNT(*) as use_count
            FROM items
            WHERE preset_name IS NOT NULL AND preset_name != ''
              AND org_id = ?
            GROUP BY preset_name
            ORDER BY use_count DESC
            LIMIT 5
        """,
            (org_id,),
        )
        top_presets = [
            {"name": row[0], "use_count": row[1]} for row in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT COUNT(*) FROM items
            WHERE preset_name IS NOT NULL AND preset_name != ''
              AND org_id = ?
        """,
            (org_id,),
        )
        total_usage = cursor.fetchone()[0]

    return jsonify(
        {
            "total_presets": total_presets,
            "total_groups": total_groups,
            "ungrouped": ungrouped,
            "avg_cost": avg_cost,
            "min_cost": min_cost,
            "max_cost": max_cost,
            "total_usage": total_usage,
            "top_presets": top_presets,
        }
    )


@app.route("/api/giveaways/insights", methods=["GET"])
def get_giveaway_insights():
    """Get giveaway insights by show and preset."""
    org_id = get_current_org_id()
    group = request.args.get("group", "monthly").lower()
    if group not in {"daily", "weekly", "monthly", "quarterly", "yearly"}:
        group = "monthly"
    now = datetime.now()
    try:
        month = int(request.args.get("month", str(now.month)))
    except ValueError:
        month = now.month
    try:
        year = int(request.args.get("year", str(now.year)))
    except ValueError:
        year = now.year
    try:
        quarter = int(request.args.get("quarter", "1"))
    except ValueError:
        quarter = 1
    quarter = min(max(quarter, 1), 4)

    rows = fetch_all(
        """
        SELECT
            shows.id,
            shows.name,
            shows.date,
            presets.name,
            COUNT(*) as giveaway_count,
            COALESCE(SUM(items.cost), 0) as total_cost
        FROM items
        JOIN shows ON shows.id = items.show_id
        JOIN presets ON presets.name = items.preset_name
        WHERE presets.is_giveaway = 1
          AND items.org_id = ?
          AND shows.org_id = ?
          AND presets.org_id = ?
        GROUP BY shows.id, shows.name, shows.date, presets.name
        ORDER BY shows.date DESC, presets.name
    """,
        (org_id, org_id, org_id),
    )

    shows_map = {}
    preset_totals = {}
    overall_count = 0
    overall_cost = 0.0

    for show_id, show_name, show_date, preset_name, count, total_cost in rows:
        try:
            show_dt = datetime.strptime(show_date, "%Y-%m-%d")
        except Exception:
            continue
        if group in {"daily", "weekly"}:
            if show_dt.year != year or show_dt.month != month:
                continue
        elif group == "monthly":
            if show_dt.year != year:
                continue
        elif group == "quarterly":
            if show_dt.year != year:
                continue
            show_quarter = (show_dt.month - 1) // 3 + 1
            if show_quarter != quarter:
                continue
        else:
            if show_dt.year != year:
                continue

        if show_id not in shows_map:
            shows_map[show_id] = {
                "show_id": show_id,
                "show_name": show_name,
                "show_date": show_date,
                "total_count": 0,
                "total_cost": 0.0,
                "presets": [],
            }
        shows_map[show_id]["presets"].append(
            {
                "preset_name": preset_name,
                "count": count,
                "total_cost": total_cost or 0.0,
            }
        )
        shows_map[show_id]["total_count"] += count
        shows_map[show_id]["total_cost"] += float(total_cost or 0.0)

        if preset_name not in preset_totals:
            preset_totals[preset_name] = {"preset_name": preset_name, "count": 0, "total_cost": 0.0}
        preset_totals[preset_name]["count"] += count
        preset_totals[preset_name]["total_cost"] += float(total_cost or 0.0)

        overall_count += count
        overall_cost += float(total_cost or 0.0)

    return jsonify(
        {
            "overall": {"total_count": overall_count, "total_cost": overall_cost},
            "shows": list(shows_map.values()),
            "presets": list(preset_totals.values()),
        }
    )


def _profit_loss_period_key(show_date: datetime, group: str):
    if group == "daily":
        return show_date.date(), f"{show_date.month}/{show_date.day}"
    if group == "weekly":
        month_days = calendar.monthrange(show_date.year, show_date.month)[1]
        week_index = (show_date.day - 1) // 7
        start_day = week_index * 7 + 1
        end_day = min(start_day + 6, month_days)
        start_date = datetime(show_date.year, show_date.month, start_day).date()
        label = f"{show_date.month}/{start_day}-{show_date.month}/{end_day}"
        return start_date, label
    if group in {"monthly", "quarterly", "yearly"}:
        return (show_date.year, show_date.month), show_date.strftime("%b")
    return (show_date.year, show_date.month), show_date.strftime("%b")


@app.route("/api/profit-loss", methods=["GET"])
def get_profit_loss():
    """Profit & loss summary grouped by show date."""
    org_id = get_current_org_id()
    group = request.args.get("group", "monthly").lower()
    if group not in {"daily", "weekly", "monthly", "quarterly", "yearly"}:
        group = "monthly"
    now = datetime.now()
    try:
        commission_rate = float(request.args.get("commission_rate", "0.08"))
    except ValueError:
        commission_rate = 0.08
    try:
        month = int(request.args.get("month", str(now.month)))
    except ValueError:
        month = now.month
    try:
        year = int(request.args.get("year", str(now.year)))
    except ValueError:
        year = now.year
    try:
        quarter = int(request.args.get("quarter", "1"))
    except ValueError:
        quarter = 1
    quarter = min(max(quarter, 1), 4)

    giveaway_map = get_preset_giveaway_map()

    shows = fetch_all(
        "SELECT id, name, date FROM shows WHERE org_id = ? ORDER BY date DESC",
        (org_id,),
    )

    periods = {}
    giveaway_types = {}

    for show_id, show_name, show_date in shows:
        try:
            show_dt = datetime.strptime(show_date, "%Y-%m-%d")
        except Exception:
            continue
        if group in {"daily", "weekly"}:
            if show_dt.year != year or show_dt.month != month:
                continue
        elif group in {"monthly"}:
            if show_dt.year != year:
                continue
        elif group == "quarterly":
            if show_dt.year != year:
                continue
            show_quarter = (show_dt.month - 1) // 3 + 1
            if show_quarter != quarter:
                continue
        else:
            if show_dt.year != year:
                continue

        period_key, label = _profit_loss_period_key(show_dt, group)
        if period_key not in periods:
            periods[period_key] = {
                "label": label,
                "sales": 0.0,
                "net_revenue": 0.0,
                "cost": 0.0,
                "giveaway_cost": 0.0,
                "hours": 0.0,
                "profit": 0.0,
            }

        item_rows = fetch_all(
            "SELECT item_name, cost, preset_name, cancelled_status FROM items WHERE show_id = ? AND org_id = ?",
            (show_id, org_id),
        )
        item_meta = {
            row[0]: {"cost": row[1], "preset_name": row[2], "status": row[3]}
            for row in item_rows
        }

        sessions = fetch_all(
            "SELECT started_at, stopped_at FROM recording_sessions WHERE show_id = ? AND org_id = ?",
            (show_id, org_id),
        )
        session_hours = 0.0
        for started_at, stopped_at in sessions:
            start_dt = parse_timestamp(started_at)
            end_dt = parse_timestamp(stopped_at) if stopped_at else datetime.now()
            if start_dt and end_dt and end_dt > start_dt:
                session_hours += (end_dt - start_dt).total_seconds() / 3600.0
        periods[period_key]["hours"] += session_hours

        _, _, _, rows = load_show_rows(show_id)
        if session_hours == 0.0:
            log_times = []
            for row in rows:
                log_timestamp = parse_timestamp(row.get("timestamp"))
                if log_timestamp:
                    log_times.append(log_timestamp)
            log_times.sort()
            if len(log_times) >= 2:
                show_seconds = (log_times[-1] - log_times[0]).total_seconds()
                if show_seconds > 0:
                    periods[period_key]["hours"] += show_seconds / 3600.0

        for row in rows:
            item_name = (row.get("item_title") or "").strip()
            if not item_name:
                continue
            sold_price = parse_number(row.get("sold_price"))
            meta = item_meta.get(item_name, {})
            status = (meta.get("status") or "").strip()
            if status in {"Cancelled", "Failed"}:
                continue
            cost_val = meta.get("cost") or 0.0
            preset_name = meta.get("preset_name")
            is_giveaway = giveaway_map.get(preset_name, False)

            if sold_price is not None:
                periods[period_key]["sales"] += sold_price
            if not is_giveaway and sold_price is not None:
                net_rev = sold_price * (1 - commission_rate - 0.029) - 0.30
                periods[period_key]["net_revenue"] += net_rev
            if is_giveaway:
                periods[period_key]["giveaway_cost"] += float(cost_val or 0.0)
                if preset_name:
                    if preset_name not in giveaway_types:
                        giveaway_types[preset_name] = {}
                    if period_key not in giveaway_types[preset_name]:
                        giveaway_types[preset_name][period_key] = {
                            "count": 0,
                            "cost": 0.0,
                        }
                    giveaway_types[preset_name][period_key]["count"] += 1
                    giveaway_types[preset_name][period_key]["cost"] += float(
                        cost_val or 0.0
                    )
            periods[period_key]["cost"] += float(cost_val or 0.0)

    # finalize profit + totals
    period_keys = sorted(periods.keys())
    period_list = [{"key": str(k), "label": periods[k]["label"]} for k in period_keys]
    rows_out = {
        "sales": [],
        "net_revenue": [],
        "cost": [],
        "giveaway_cost": [],
        "hours": [],
        "profit": [],
    }
    totals = {k: 0.0 for k in rows_out.keys()}

    for key in period_keys:
        data = periods[key]
        data["profit"] = data["net_revenue"] - data["cost"]
        for field in rows_out.keys():
            rows_out[field].append(data[field])
            totals[field] += data[field]

    giveaway_rows = {}
    for preset_name, by_period in giveaway_types.items():
        giveaway_rows[preset_name] = {
            "costs": [],
            "counts": [],
        }
        for key in period_keys:
            entry = by_period.get(key, {"count": 0, "cost": 0.0})
            giveaway_rows[preset_name]["counts"].append(entry["count"])
            giveaway_rows[preset_name]["costs"].append(entry["cost"])

    return jsonify(
        {
            "periods": period_list,
            "rows": rows_out,
            "giveaway_types": giveaway_rows,
            "totals": totals,
        }
    )


@app.route("/api/items/cost", methods=["POST"])
def set_items_cost():
    """Set cost and optionally preset_name, sku, notes, buyer, order_id, cancelled_status for one or more items. Cost can be null to clear it."""
    data = request.json
    item_names = data.get("item_names", [])
    cost = data.get("cost")
    preset_name = data["preset_name"] if "preset_name" in data else _UNSET
    sku = data["sku"] if "sku" in data else _UNSET
    notes = data["notes"] if "notes" in data else _UNSET
    buyer = data["buyer"] if "buyer" in data else _UNSET
    order_id = data["order_id"] if "order_id" in data else _UNSET
    cancelled_status = (
        data["cancelled_status"] if "cancelled_status" in data else _UNSET
    )
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    
    # Handle both single item (string) and multiple items (list)
    if isinstance(item_names, str):
        item_names = [item_names]
    
    if not item_names:
        return jsonify({"error": "Item names required"}), 400
    
    # Allow cost to be None/null (to clear it)
    cost_float = None
    if cost is not None:
        try:
            cost_float = float(cost)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid cost"}), 400
    
    for item_name in item_names:
        set_item_cost(
            item_name,
            cost_float,
            show_id,
            preset_name,
            sku,
            notes,
            buyer,
            order_id,
            cancelled_status,
        )

    return jsonify({"success": True})


@app.route("/api/items/sku", methods=["POST"])
def set_item_sku():
    """Set SKU for an item."""
    data = request.json
    item_name = data.get("item_name")
    sku = (data.get("sku") or "").strip() or None
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    
    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    set_item_cost(
        item_name,
        _UNSET,
        show_id,
        preset_name=_UNSET,
        sku=sku,
        notes=_UNSET,
        buyer=_UNSET,
        order_id=_UNSET,
        cancelled_status=_UNSET,
    )
    return jsonify({"success": True})


@app.route("/api/items/notes", methods=["POST"])
def set_item_notes():
    """Set notes for an item."""
    data = request.json
    item_name = data.get("item_name")
    notes = (data.get("notes") or "").strip() or None
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    
    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    set_item_cost(
        item_name,
        _UNSET,
        show_id,
        preset_name=_UNSET,
        sku=_UNSET,
        notes=notes,
        buyer=_UNSET,
        order_id=_UNSET,
        cancelled_status=_UNSET,
    )
    return jsonify({"success": True})


@app.route("/api/items/buyer", methods=["POST"])
def set_item_buyer():
    """Set buyer for an item."""
    data = request.json
    item_name = data.get("item_name")
    buyer = (data.get("buyer") or "").strip() or None
    show_id = data.get("show_id", request.args.get("show_id", type=int))

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    set_item_cost(
        item_name,
        _UNSET,
        show_id,
        preset_name=_UNSET,
        sku=_UNSET,
        notes=_UNSET,
        buyer=buyer,
        order_id=_UNSET,
        cancelled_status=_UNSET,
    )
    return jsonify({"success": True})


@app.route("/api/items/order-id", methods=["POST"])
def set_item_order_id():
    """Set order ID for an item."""
    data = request.json
    item_name = data.get("item_name")
    order_id = (data.get("order_id") or "").strip() or None
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    
    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    set_item_cost(
        item_name,
        _UNSET,
        show_id,
        preset_name=_UNSET,
        sku=_UNSET,
        notes=_UNSET,
        buyer=_UNSET,
        order_id=order_id,
        cancelled_status=_UNSET,
    )
    return jsonify({"success": True})


@app.route("/api/items/cancelled-status", methods=["POST"])
def set_item_cancelled_status():
    """Set cancelled/failed status for an item."""
    data = request.json
    item_name = data.get("item_name")
    cancelled_status = data.get("cancelled_status", "").strip() or None
    show_id = data.get("show_id", request.args.get("show_id", type=int))

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    set_item_cost(
        item_name,
        _UNSET,
        show_id,
        preset_name=_UNSET,
        sku=_UNSET,
        notes=_UNSET,
        buyer=_UNSET,
        order_id=_UNSET,
        cancelled_status=cancelled_status,
    )
    return jsonify({"success": True})


@app.route("/api/items/sold-price", methods=["POST"])
def update_sold_price():
    """Update sold price for an item in CSV."""
    data = request.json
    item_name = data.get("item_name")
    sold_price = (data.get("sold_price") or data.get("sale_price") or "").strip()
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    org_id = get_current_org_id()

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    try:
        ok, err = show_utils.update_csv_column(show_id, org_id, item_name, "sold_price", sold_price)
        if not ok:
            return jsonify({"error": err}), 404
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": "Failed to update CSV"}), 500


@app.route("/api/items/sold-time", methods=["POST"])
def update_sold_time():
    """Update sold timestamp for an item in CSV."""
    data = request.json or {}
    item_name = (data.get("item_name") or "").strip()
    sold_timestamp = (data.get("sold_timestamp") or "").strip()
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    org_id = get_current_org_id()

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    try:
        ok, err = show_utils.update_csv_column(show_id, org_id, item_name, "sold_timestamp", sold_timestamp)
        if not ok:
            return jsonify({"error": err}), 404
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": "Failed to update CSV"}), 500


@app.route("/api/items/viewers", methods=["POST"])
def update_viewers():
    """Update viewers for an item in CSV."""
    data = request.json or {}
    item_name = (data.get("item_name") or "").strip()
    viewers_val = (data.get("viewers") or "").strip()
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    org_id = get_current_org_id()

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    try:
        ok, err = show_utils.update_csv_column(show_id, org_id, item_name, "viewers", viewers_val)
        if not ok:
            return jsonify({"error": err}), 404
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": "Failed to update CSV"}), 500


@app.route("/api/items/pinned-message", methods=["POST"])
def update_pinned_message():
    """Update pinned message text for an item in CSV."""
    data = request.json
    item_name = data.get("item_name")
    pinned_message = (data.get("pinned_message") or "").strip()
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    org_id = get_current_org_id()

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    try:
        ok, err = show_utils.update_csv_column(show_id, org_id, item_name, "pinned_text", pinned_message)
        if not ok:
            return jsonify({"error": err}), 404
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": "Failed to update CSV"}), 500


@app.route("/api/items/delete", methods=["POST"])
def delete_items():
    """Delete items from CSV by item names."""
    data = request.json
    item_names = data.get("item_names", [])
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    org_id = get_current_org_id()
    
    if not item_names:
        return jsonify({"error": "Item names required"}), 400

    # Get show-specific CSV file
    csv_file = None
    if show_id:
        # Get show info from database to find the show folder
        result = fetch_one(
            "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
            (show_id, org_id),
        )

        if result:
            show_name, show_date = result
            show_dir = show_dir_path(show_name, show_date)
            csv_file = show_dir / "log.csv"

    # Fallback to global CSV if no show_id
    if not csv_file or not csv_file.exists():
        csv_file = CSV_FILE

    if not csv_file.exists():
        return jsonify({"error": "CSV file not found"}), 404

    try:
        # Read CSV
        rows = []
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        if not rows:
            return jsonify({"error": "CSV file is empty"}), 400
        
        header = rows[0]
        
        # Find item_title column index (should be column 1)
        try:
            item_title_idx = header.index("item_title")
        except ValueError:
            item_title_idx = 1  # Default to column 1
        
        # Filter out rows matching item names
        item_names_set = set(item_names)
        filtered_rows = [rows[0]]  # Keep header
        deleted_count = 0
        
        for row in rows[1:]:  # Skip header
            if len(row) > item_title_idx:
                item_name = row[item_title_idx].strip()
                if item_name not in item_names_set:
                    filtered_rows.append(row)
                else:
                    deleted_count += 1
        
        if deleted_count == 0:
            return jsonify({"error": "No items found to delete"}), 404
        
        # Write back
        with open(csv_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(filtered_rows)
        
        # Also delete costs from database (with show_id if provided)
        with get_db() as (conn, cursor):
            for item_name in item_names:
                if show_id:
                    cursor.execute(
                        "DELETE FROM items WHERE item_name = ? AND show_id = ? AND org_id = ?",
                        (item_name, show_id, org_id),
                    )
                else:
                    cursor.execute(
                        "DELETE FROM items WHERE item_name = ? AND org_id = ?",
                        (item_name, org_id),
                    )
            conn.commit()

        return jsonify({"success": True, "deleted_count": deleted_count})
    
    except Exception as e:
        return jsonify({"error": f"Failed to delete items: {str(e)}"}), 500


@app.route("/api/items/add", methods=["POST"])
def add_item():
    """Add a manual item row to the CSV."""
    data = request.json or {}
    item_name = (data.get("item_name") or "").strip()
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    pinned_text = (data.get("pinned_text") or "").strip()
    filename = (data.get("filename") or "").strip() or "__no_image__"
    sold_price = (data.get("sold_price") or "").strip()
    sold_timestamp = (data.get("sold_timestamp") or "").strip()
    viewers = (data.get("viewers") or "").strip()
    org_id = get_current_org_id()

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    # Get show-specific CSV file
    csv_file = None
    if show_id:
        result = fetch_one(
            "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
            (show_id, org_id),
        )
        if result:
            show_name, show_date = result
            show_dir = show_dir_path(show_name, show_date)
            csv_file = show_dir / "log.csv"

    if not csv_file:
        csv_file = CSV_FILE

    # Ensure file exists with header
    if not csv_file.exists():
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "timestamp",
                    "item_title",
                    "pinned_text",
                    "filename",
                    "sold_price",
                    "sold_timestamp",
                    "viewers",
                ]
            )

    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        header = rows[0] if rows else []
        required_cols = [
            "timestamp",
            "item_title",
            "pinned_text",
            "filename",
            "sold_price",
            "sold_timestamp",
            "viewers",
        ]
        updated = False
        for col in required_cols:
            if col not in header:
                header.append(col)
                updated = True
        if updated:
            if rows:
                rows[0] = header
            else:
                rows = [header]
            num_columns = len(header)
            for i in range(1, len(rows)):
                while len(rows[i]) < num_columns:
                    rows[i].append("")

        col_index = {name: idx for idx, name in enumerate(header)}

        existing_names = set()
        for row in rows[1:]:
            if len(row) > col_index["item_title"]:
                existing_names.add(row[col_index["item_title"]].strip())

        final_name = item_name
        if final_name in existing_names:
            counter = 2
            while f"{item_name} ({counter})" in existing_names:
                counter += 1
            final_name = f"{item_name} ({counter})"

        new_row = [""] * len(header)
        new_row[col_index["timestamp"]] = datetime.now().isoformat()
        new_row[col_index["item_title"]] = final_name
        new_row[col_index["pinned_text"]] = pinned_text
        new_row[col_index["filename"]] = filename
        new_row[col_index["sold_price"]] = sold_price
        new_row[col_index["sold_timestamp"]] = sold_timestamp
        new_row[col_index["viewers"]] = viewers
        rows.append(new_row)

        with open(csv_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        sold_price_float = None
        if sold_price:
            price_match = re.search(r"[\d,]+\.?\d*", sold_price)
            if price_match:
                try:
                    sold_price_float = float(price_match.group().replace(",", ""))
                except ValueError:
                    pass

        image_path = None
        if filename.startswith("http://") or filename.startswith("https://"):
            image_path = filename
        elif filename.startswith("data:image"):
            image_path = filename

        return jsonify(
            {
                "success": True,
                "item": {
                    "item_name": final_name,
                    "pinned_message": pinned_text,
                    "sold_price": sold_price,
                    "sold_price_float": sold_price_float,
                    "sold_timestamp": sold_timestamp,
                    "cost": None,
                    "preset_name": None,
                    "sku": None,
                    "notes": None,
                    "buyer": None,
                    "order_id": None,
                    "cancelled_status": None,
                    "viewers": viewers,
                    "image": image_path,
                    "timestamp": new_row[col_index["timestamp"]],
                    "filename": filename,
                },
            }
        )
    except Exception as e:
        return jsonify({"error": f"Failed to add item: {str(e)}"}), 500


@app.route("/api/items/image", methods=["POST"])
def update_item_image():
    """Update image reference for an item in CSV."""
    data = request.json or {}
    item_name = (data.get("item_name") or "").strip()
    image_ref = (data.get("image_ref") or "").strip()
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    org_id = get_current_org_id()

    if not item_name:
        return jsonify({"error": "Item name required"}), 400

    # Get show-specific CSV file
    csv_file = None
    if show_id:
        result = fetch_one(
            "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
            (show_id, org_id),
        )
        if result:
            show_name, show_date = result
            show_dir = show_dir_path(show_name, show_date)
            csv_file = show_dir / "log.csv"

    if not csv_file:
        csv_file = CSV_FILE

    if not csv_file.exists():
        return jsonify({"error": "CSV file not found"}), 404

    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            return jsonify({"error": "CSV file is empty"}), 400

        header = rows[0]
        if "filename" not in header:
            header.append("filename")
            rows[0] = header

        try:
            item_title_idx = header.index("item_title")
        except ValueError:
            item_title_idx = 1

        filename_idx = header.index("filename")
        num_columns = len(header)
        for i in range(1, len(rows)):
            while len(rows[i]) < num_columns:
                rows[i].append("")

        updated = False
        for i in range(len(rows) - 1, 0, -1):
            if len(rows[i]) > item_title_idx and rows[i][item_title_idx] == item_name:
                rows[i][filename_idx] = image_ref or "__no_image__"
                updated = True
                break

        if not updated:
            return jsonify({"error": "Item not found in CSV"}), 404

        with open(csv_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": f"Failed to update image: {str(e)}"}), 500


@app.route("/api/items/restore", methods=["POST"])
def restore_items():
    """Restore deleted items by appending rows back to CSV."""
    data = request.json or {}
    rows_to_restore = data.get("rows", [])
    show_id = data.get("show_id", request.args.get("show_id", type=int))
    org_id = get_current_org_id()

    if not rows_to_restore:
        return jsonify({"error": "Rows required"}), 400

    # Get show-specific CSV file
    csv_file = None
    if show_id:
        result = fetch_one(
            "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
            (show_id, org_id),
        )
        if result:
            show_name, show_date = result
            show_dir = show_dir_path(show_name, show_date)
            csv_file = show_dir / "log.csv"

    if not csv_file:
        csv_file = CSV_FILE

    if not csv_file.exists():
        return jsonify({"error": "CSV file not found"}), 404

    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            return jsonify({"error": "CSV file is empty"}), 400

        header = rows[0]
        required_cols = [
            "timestamp",
            "item_title",
            "pinned_text",
            "filename",
            "sold_price",
            "sold_timestamp",
            "viewers",
        ]
        updated = False
        for col in required_cols:
            if col not in header:
                header.append(col)
                updated = True

        if updated:
            rows[0] = header
            num_columns = len(header)
            for i in range(1, len(rows)):
                while len(rows[i]) < num_columns:
                    rows[i].append("")

        col_index = {name: idx for idx, name in enumerate(header)}

        for entry in rows_to_restore:
            new_row = [""] * len(header)
            new_row[col_index["timestamp"]] = (
                entry.get("timestamp") or datetime.now().isoformat()
            )
            new_row[col_index["item_title"]] = entry.get("item_title") or ""
            new_row[col_index["pinned_text"]] = entry.get("pinned_text") or ""
            new_row[col_index["filename"]] = entry.get("filename") or "__no_image__"
            new_row[col_index["sold_price"]] = entry.get("sold_price") or ""
            new_row[col_index["sold_timestamp"]] = entry.get("sold_timestamp") or ""
            new_row[col_index["viewers"]] = entry.get("viewers") or ""
            rows.append(new_row)

        with open(csv_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        return jsonify({"success": True, "restored": len(rows_to_restore)})
    except Exception as e:
        return jsonify({"error": f"Failed to restore CSV: {str(e)}"}), 500


@app.route("/db-image/<int:show_id>/<path:item_name>")
def serve_db_image(show_id, item_name):
    """Serve item screenshot from DB (for Render ephemeral disk)."""
    from flask import Response
    from urllib.parse import unquote
    item_name = unquote(item_name)
    row = fetch_one(
        "SELECT image_data FROM items WHERE show_id = ? AND item_name = ?",
        (show_id, item_name),
    )
    if row and row[0]:
        data = row[0]
        if isinstance(data, memoryview):
            data = bytes(data)
        return Response(data, mimetype="image/png")
    return "Not found", 404


@app.route("/screenshots/<path:filename>")
def serve_screenshot(filename):
    """Serve screenshot images from show-specific folders."""
    from urllib.parse import unquote

    filename = unquote(filename)
    
    # Filename might be: "show_name_date/image.png" or just "image.png"
    # Handle both cases
    if "/" in filename:
        # Has show folder in path
        image_path = CAPTURES_DIR / filename
    else:
        # Old format - look in captures directory
        image_path = CAPTURES_DIR / filename
    
    if image_path.exists() and image_path.is_file():
        from flask import send_file

        return send_file(image_path)
    return jsonify({"error": "Image not found"}), 404


@app.route("/thumbnails/<path:filename>")
def serve_thumbnail(filename):
    """Serve compressed JPEG thumbnails for print/PDF use. Much smaller than full PNGs."""
    from urllib.parse import unquote
    import io
    from PIL import Image as PILImage

    filename = unquote(filename)
    image_path = CAPTURES_DIR / filename

    if not image_path.exists() or not image_path.is_file():
        return jsonify({"error": "Image not found"}), 404

    try:
        img = PILImage.open(image_path)
        img = img.convert("RGB")
        # Resize to max 240px (2x display size for sharpness)
        img.thumbnail((240, 240))
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=70, optimize=True)
        buffer.seek(0)
        return send_file(buffer, mimetype="image/jpeg")
    except Exception:
        # Fallback: serve original file
        return send_file(image_path)


@app.route("/api/items/export", methods=["GET"])
def export_items():
    """Export items as CSV for a specific show."""
    show_id = request.args.get("show_id", type=int)
    org_id = get_current_org_id()

    if not show_id:
        return jsonify({"error": "Show ID required"}), 400

    # Get show info from database
    result = fetch_one(
        "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
        (show_id, org_id),
    )

    if not result:
        return jsonify({"error": "Show not found"}), 404

    show_name, show_date = result

    # Create show-specific folder path
    show_dir = show_dir_path(show_name, show_date)
    csv_file = show_dir / "log.csv"

    if not csv_file.exists():
        return jsonify({"error": "CSV file not found"}), 404

    # Read CSV and enhance with database data
    output_rows = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            item_name = row.get("item_title", "").strip()
            if not item_name:
                continue

            # Get cost, preset_name, sku, notes, buyer, order_id, cancelled_status from database
            db_result = fetch_one(
                "SELECT cost, preset_name, sku, notes, buyer, order_id, cancelled_status FROM items WHERE item_name = ? AND show_id = ? AND org_id = ?",
                (item_name, show_id, org_id),
            )

            cost = db_result[0] if db_result and db_result[0] is not None else ""
            preset_name = db_result[1] if db_result and db_result[1] else ""
            sku = db_result[2] if db_result and db_result[2] else ""
            notes = db_result[3] if db_result and db_result[3] else ""
            buyer = db_result[4] if db_result and db_result[4] else ""
            order_id = db_result[5] if db_result and db_result[5] else ""
            cancelled_status = db_result[6] if db_result and db_result[6] else ""

            # Build output row with all columns
            output_rows.append(
                {
                    "item_title": item_name,
                    "pinned_text": row.get("pinned_text", ""),
                    "filename": row.get("filename", ""),
                    "sold_price": row.get("sold_price", ""),
                    "sold_timestamp": row.get("sold_timestamp", ""),
                    "viewers": row.get("viewers", ""),
                    "cost": str(cost) if cost else "",
                    "preset_name": preset_name,
                    "sku": sku,
                    "notes": notes,
                    "buyer": buyer,
                    "order_id": order_id,
                    "cancelled_status": cancelled_status,
                }
            )

    # Create CSV in memory
    import io

    output = io.StringIO()
    fieldnames = [
        "item_title",
        "pinned_text",
        "filename",
        "sold_price",
        "sold_timestamp",
        "viewers",
        "cost",
        "preset_name",
        "sku",
        "notes",
        "buyer",
        "order_id",
        "cancelled_status",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

    # Create response
    filename = f"{safe_filename(show_name)}_{safe_filename(show_date)}_export.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/api/shows/<int:show_id>/summary", methods=["GET"])
def show_summary_api(show_id):
    commission_rate = request.args.get("commission_rate", default=8, type=float) / 100.0
    processed_only = request.args.get("processed_only", "1") == "1"
    summary = compute_show_summary(
        show_id, commission_rate=commission_rate, processed_only=processed_only
    )
    if not summary:
        return jsonify({"error": "Show not found"}), 404
    return jsonify(summary)


@app.route("/api/shows/<int:show_id>/summary.csv", methods=["GET"])
def show_summary_csv(show_id):
    commission_rate = request.args.get("commission_rate", default=8, type=float) / 100.0
    processed_only = request.args.get("processed_only", "1") == "1"
    summary = compute_show_summary(
        show_id, commission_rate=commission_rate, processed_only=processed_only
    )
    if not summary:
        return jsonify({"error": "Show not found"}), 404

    import io

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Metric", "Value"])
    writer.writerow(["Show name", summary["show_name"]])
    writer.writerow(["Show date", summary["show_date"]])
    writer.writerow(["Show time (minutes)", summary["show_minutes"] or "N/A"])
    writer.writerow(["Average time between sales (seconds)", summary["avg_time_between_sales"] or "N/A"])
    writer.writerow(["Total revenue (pre fees)", f'{summary["total_revenue_pre_fees"]:.2f}'])
    writer.writerow(["Revenue (post fees)", f'{summary["total_revenue_post_fees"]:.2f}'])
    writer.writerow(["Total cost", f'{summary["total_cost"]:.2f}'])
    writer.writerow(["Total profit", f'{summary["total_profit"]:.2f}'])
    writer.writerow(["AOV", f'{summary["aov"]:.2f}'])
    writer.writerow(["AOC", f'{summary["aoc"]:.2f}'])
    writer.writerow(["Average profit", f'{summary["avg_profit"]:.2f}'])
    writer.writerow(["Items cancelled", summary["cancelled_count"]])
    writer.writerow(["Items failed", summary["failed_count"]])
    writer.writerow(["Average viewers", f'{summary["avg_viewers"]:.2f}'])
    writer.writerow(["Sales per minute", f'{summary["sales_per_minute"]:.4f}' if summary["sales_per_minute"] is not None else "N/A"])
    writer.writerow(["Minutes per sale", f'{summary["minutes_per_sale"]:.4f}' if summary["minutes_per_sale"] is not None else "N/A"])

    filename = f"{safe_filename(summary['show_name'])}_{safe_filename(summary['show_date'])}_summary.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/shows/<int:show_id>/summary", methods=["GET"])
@login_required
def show_summary_page(show_id):
    commission_rate = request.args.get("commission_rate", default=8, type=float) / 100.0
    processed_only = request.args.get("processed_only", "1") == "1"
    summary = compute_show_summary(
        show_id, commission_rate=commission_rate, processed_only=processed_only
    )
    if not summary:
        return "Show not found", 404
    return render_template("summary.html", summary=summary)


@app.route("/api/items/import", methods=["POST"])
def import_items():
    """Import items from CSV file and associate with show_id."""
    show_id = request.form.get("show_id", type=int)
    org_id = get_current_org_id()

    if not show_id:
        return jsonify({"error": "Show ID required"}), 400

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if not file.filename.endswith(".csv"):
        return jsonify({"error": "File must be a CSV"}), 400

    # Get show info from database
    result = fetch_one(
        "SELECT name, date FROM shows WHERE id = ? AND org_id = ?",
        (show_id, org_id),
    )

    if not result:
        return jsonify({"error": "Show not found"}), 404

    show_name, show_date = result

    # Create show-specific folder path
    show_dir = show_dir_path(show_name, show_date)
    show_dir.mkdir(exist_ok=True)
    csv_file = show_dir / "log.csv"

    try:
        # Read uploaded CSV safely (avoid SpooledTemporaryFile issues)
        import io

        raw = file.read()
        if isinstance(raw, bytes):
            text = raw.decode("utf-8", errors="ignore")
        else:
            text = str(raw)
        stream = io.StringIO(text)
        reader = csv.DictReader(stream)

        def normalize_key(key):
            return (key or "").strip().lower()

        def normalize_price(value):
            if value is None:
                return ""
            value = str(value).strip()
            if not value:
                return ""
            # Extract numeric portion
            cleaned = re.sub(r"[^\d\.]", "", value)
            if not cleaned:
                return ""
            try:
                num = float(cleaned)
                return f"${num:.2f}"
            except ValueError:
                return value

        def normalize_cancelled(value):
            if value is None:
                return ""
            value = str(value).strip()
            if not value:
                return ""
            lowered = value.lower()
            if "cancel" in lowered:
                return "Cancelled"
            if "fail" in lowered:
                return "Failed"
            return value

        # Load preset lookup (by name, case-insensitive)
        preset_rows = fetch_all("SELECT name, cost FROM presets WHERE org_id = ?", (org_id,))
        preset_map = {name.lower(): {"name": name, "cost": cost} for name, cost in preset_rows}

        imported_rows = []
        imported_count = 0
        updated_count = 0

        for raw_row in reader:
            row = {normalize_key(k): (v or "").strip() for k, v in raw_row.items()}

            item_name = (
                row.get("product name")
                or row.get("product_name")
                or row.get("item_title")
                or row.get("item name")
                or ""
            )
            item_name = item_name.strip()
            if not item_name:
                continue

            sold_price = normalize_price(
                row.get("sold price") or row.get("sold_price") or ""
            )
            viewers = row.get("viewers") or ""
            cost_value = row.get("cost per item") or row.get("cost") or ""
            buyer = row.get("buyer", "")
            order_id = row.get("order numeric id") or row.get("order_id") or row.get("order id") or ""
            cancelled_status = normalize_cancelled(
                row.get("cancelled or failed") or row.get("cancelled_or_failed") or row.get("cancelled/failed") or row.get("cancelled_status") or ""
            )
            sku = row.get("sku", "")
            preset_raw = row.get("preset") or ""
            preset_match = None
            if preset_raw:
                preset_match = preset_map.get(preset_raw.strip().lower())

            imported_rows.append(
                {
                    "item_title": item_name,
                    "sold_price": sold_price,
                    "viewers": viewers,
                    "cost": cost_value,
                    "buyer": buyer,
                    "order_id": order_id,
                    "cancelled_status": cancelled_status,
                    "sku": sku,
                    "preset_name": preset_match["name"] if preset_match else "",
                    "preset_cost": preset_match["cost"] if preset_match else None,
                }
            )
            imported_count += 1

        # Load existing CSV rows to merge by product name
        existing_rows = []
        if csv_file.exists():
            with open(csv_file, "r", encoding="utf-8") as f:
                existing_reader = csv.DictReader(f)
                existing_rows = list(existing_reader)

        existing_by_name = {
            row.get("item_title", "").strip(): row for row in existing_rows
        }
        merged_rows = existing_rows.copy()

        for row in imported_rows:
            item_name = row.get("item_title", "").strip()
            if not item_name:
                continue

            # Update DB fields only if provided
            cost_raw = row.get("cost", "").strip()
            buyer = row.get("buyer", "").strip()
            order_id = row.get("order_id", "").strip()
            cancelled_status = row.get("cancelled_status", "").strip()
            sku_raw = row.get("sku", "")
            sku_value = sku_raw.strip()
            preset_name = row.get("preset_name", "").strip()
            preset_cost = row.get("preset_cost", None)
            sku_has_value = bool(sku_value)

            if cost_raw or buyer or order_id or cancelled_status or sku_has_value or preset_name:
                cost_float = _UNSET
                if cost_raw:
                    try:
                        cost_float = float(
                            str(cost_raw).replace("$", "").replace(",", "").strip()
                        )
                    except (ValueError, AttributeError):
                        cost_float = _UNSET
                elif preset_name and preset_cost is not None:
                    cost_float = preset_cost

                set_item_cost(
                    item_name,
                    cost_float,
                    show_id,
                    preset_name=preset_name if preset_name else _UNSET,
                    sku=sku_value if sku_has_value else _UNSET,
                    notes=_UNSET,
                    buyer=buyer if buyer else _UNSET,
                    order_id=order_id if order_id else _UNSET,
                    cancelled_status=cancelled_status if cancelled_status else _UNSET,
                )
                updated_count += 1

            if item_name in existing_by_name:
                # Merge into existing CSV row
                existing_row = existing_by_name[item_name]
                # If there was no filename, lock it to prevent auto image matching
                if not (existing_row.get("filename") or "").strip():
                    existing_row["filename"] = "__no_image__"
                # Only override sold_price if CSV provides a value
                if row.get("sold_price"):
                    existing_row["sold_price"] = row.get("sold_price")
                if row.get("viewers"):
                    existing_row["viewers"] = row.get("viewers")
                # Keep sold_timestamp as-is
            else:
                # Add new row for products not in dashboard
                merged_rows.append(
                    {
                        "timestamp": "",
                        "item_title": item_name,
                        "pinned_text": "",
                        "filename": "__no_image__",
                        "sold_price": row.get("sold_price", ""),
                        "sold_timestamp": "",
                        "viewers": row.get("viewers", ""),
                    }
                )

        # Write merged CSV safely: write to temp file first, then replace
        fieldnames = [
            "timestamp",
            "item_title",
            "pinned_text",
            "filename",
            "sold_price",
            "sold_timestamp",
            "viewers",
        ]
        temp_csv = csv_file.with_suffix(".log.csv.tmp")
        with open(temp_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(merged_rows)
        if csv_file.exists():
            backup_name = f"{csv_file.stem}.backup_{int(time.time())}.csv"
            backup_path = csv_file.with_name(backup_name)
            shutil.copy2(csv_file, backup_path)
        os.replace(temp_csv, csv_file)

        return jsonify(
            {
                "success": True,
                "imported": imported_count,
                "updated": updated_count,
                "message": f"Successfully imported {imported_count} items",
            }
        )

    except Exception as e:
        return jsonify({"error": f"Error importing CSV: {str(e)}"}), 500



@app.route("/api/export-all", methods=["GET"])
@login_required
def export_all_data():
    """Export all shows, items, presets, and preset groups as JSON (owner only)."""
    if not require_owner():
        return jsonify({"error": "Forbidden"}), 403
    org_id = get_current_org_id()

    shows = fetch_all("SELECT id, name, date FROM shows WHERE org_id = ? ORDER BY id", (org_id,))
    presets = fetch_all("SELECT name, cost, is_giveaway FROM presets WHERE org_id = ?", (org_id,))
    groups = fetch_all("SELECT name FROM preset_groups WHERE org_id = ?", (org_id,))

    # Export items with full data (merge CSV + database)
    all_items = []
    for show_row in shows:
        show_id_val, show_name, show_date = show_row[0], show_row[1], show_row[2]
        show_dir = show_dir_path(show_name, show_date)
        csv_file = show_dir / "log.csv"

        if csv_file.exists():
            # Read from CSV to get sold_price, sold_timestamp, etc.
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    item_name = row.get("item_title", "").strip()
                    if not item_name:
                        continue
                    # Get DB fields
                    db_row = fetch_one(
                        "SELECT cost, preset_name, sku, notes, buyer, order_id, cancelled_status FROM items WHERE item_name = ? AND show_id = ? AND org_id = ?",
                        (item_name, show_id_val, org_id),
                    )
                    all_items.append({
                        "item_name": item_name, "show_id": show_id_val,
                        "cost": db_row[0] if db_row else None,
                        "preset_name": db_row[1] if db_row else None,
                        "sku": db_row[2] if db_row else None,
                        "notes": db_row[3] if db_row else None,
                        "buyer": db_row[4] if db_row else None,
                        "order_id": db_row[5] if db_row else None,
                        "cancelled_status": db_row[6] if db_row else None,
                        "sold_price": row.get("sold_price", row.get("sold price", "")).strip(),
                        "sold_timestamp": row.get("sold_timestamp", row.get("sold_time", "")).strip(),
                        "viewers": row.get("viewers", "").strip(),
                        "filename": row.get("filename", row.get("file", "")).strip(),
                        "pinned_message": row.get("pinned_text", row.get("pinned message", "")).strip(),
                    })
        else:
            # No CSV — export from DB
            db_items = fetch_all(
                """SELECT item_name, cost, preset_name, sku, notes, buyer, order_id,
                          cancelled_status, sold_price, sold_timestamp, viewers, filename, pinned_message
                   FROM items WHERE show_id = ? AND org_id = ?""",
                (show_id_val, org_id),
            )
            for r in db_items:
                all_items.append({
                    "item_name": r[0], "show_id": show_id_val, "cost": r[1],
                    "preset_name": r[2], "sku": r[3], "notes": r[4], "buyer": r[5],
                    "order_id": r[6], "cancelled_status": r[7], "sold_price": r[8],
                    "sold_timestamp": r[9], "viewers": r[10], "filename": r[11],
                    "pinned_message": r[12],
                })

    data = {
        "shows": [{"id": r[0], "name": r[1], "date": r[2]} for r in shows],
        "items": all_items,
        "presets": [{"name": r[0], "cost": r[1], "is_giveaway": r[2]} for r in presets],
        "preset_groups": [{"name": r[0]} for r in groups],
    }
    return jsonify(data)


@app.route("/api/import-all", methods=["POST"])
@login_required
def import_all_data():
    """Import shows, items, presets from JSON (owner only). Used to migrate data to a new database."""
    if not require_owner():
        return jsonify({"error": "Forbidden"}), 403
    org_id = get_current_org_id()
    data = request.json
    if not data:
        return jsonify({"error": "No data"}), 400

    try:
        # Map old show IDs to new ones
        show_id_map = {}
        for show in data.get("shows", []):
            old_id = show["id"]
            new_id = execute_returning_id(
                "INSERT INTO shows (org_id, name, date) VALUES (?, ?, ?)",
                (org_id, show["name"], show["date"]),
            )
            show_id_map[old_id] = new_id

        # Import items with remapped show IDs.
        # Shows were freshly inserted above so conflicts on (item_name, show_id)
        # aren't expected, but use ON CONFLICT DO NOTHING just in case a retry
        # hits a partially-imported state. Plain INSERT never wipes image_data
        # (unlike INSERT OR REPLACE, which is DELETE + INSERT).
        for item in data.get("items", []):
            new_show_id = show_id_map.get(item["show_id"])
            if not new_show_id:
                continue
            execute(
                """INSERT INTO items
                   (item_name, show_id, org_id, cost, preset_name, sku, notes, buyer, order_id, cancelled_status,
                    sold_price, sold_timestamp, viewers, filename, pinned_message)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (item_name, show_id) DO NOTHING""",
                (item["item_name"], new_show_id, org_id, item.get("cost"), item.get("preset_name"),
                 item.get("sku"), item.get("notes"), item.get("buyer"), item.get("order_id"), item.get("cancelled_status"),
                 item.get("sold_price"), item.get("sold_timestamp"), item.get("viewers"),
                 item.get("filename"), item.get("pinned_message")),
            )

        # Import presets
        for preset in data.get("presets", []):
            try:
                execute(
                    "INSERT INTO presets (org_id, name, cost, is_giveaway) VALUES (?, ?, ?, ?)",
                    (org_id, preset["name"], preset["cost"], preset.get("is_giveaway", 0)),
                )
            except Exception:
                pass  # Skip duplicates

        # Import preset groups
        for group in data.get("preset_groups", []):
            try:
                execute("INSERT INTO preset_groups (org_id, name) VALUES (?, ?)", (org_id, group["name"]))
            except Exception:
                pass

        return jsonify({"success": True, "shows_imported": len(show_id_map)})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/import-items-bulk", methods=["POST"])
@login_required
def import_items_bulk():
    """Bulk import items with pre-mapped show_ids (no show creation). Owner only."""
    if not require_owner():
        return jsonify({"error": "Forbidden"}), 403
    org_id = get_current_org_id()
    data = request.json
    if not data or "items" not in data:
        return jsonify({"error": "No items"}), 400
    try:
        count = 0
        for item in data["items"]:
            # True upsert: overwrite the 15 listed columns on conflict, but
            # preserve image_data (the 16th column). Previously INSERT OR
            # REPLACE was wiping image_data on every CSV re-import.
            execute(
                """INSERT INTO items
                   (item_name, show_id, org_id, cost, preset_name, sku, notes, buyer, order_id, cancelled_status,
                    sold_price, sold_timestamp, viewers, filename, pinned_message)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (item_name, show_id) DO UPDATE SET
                     org_id = EXCLUDED.org_id,
                     cost = EXCLUDED.cost,
                     preset_name = EXCLUDED.preset_name,
                     sku = EXCLUDED.sku,
                     notes = EXCLUDED.notes,
                     buyer = EXCLUDED.buyer,
                     order_id = EXCLUDED.order_id,
                     cancelled_status = EXCLUDED.cancelled_status,
                     sold_price = EXCLUDED.sold_price,
                     sold_timestamp = EXCLUDED.sold_timestamp,
                     viewers = EXCLUDED.viewers,
                     filename = EXCLUDED.filename,
                     pinned_message = EXCLUDED.pinned_message""",
                (item["item_name"], item["show_id"], org_id, item.get("cost"), item.get("preset_name"),
                 item.get("sku"), item.get("notes"), item.get("buyer"), item.get("order_id"), item.get("cancelled_status"),
                 item.get("sold_price"), item.get("sold_timestamp"), item.get("viewers"),
                 item.get("filename"), item.get("pinned_message")),
            )
            count += 1
        return jsonify({"success": True, "items_imported": count})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


# ─── Debug / Observability Endpoints ─────────────────────────────
#
# These two endpoints exist specifically to make Render-side issues
# diagnosable in real time without needing Render dashboard access:
#
#   GET  /api/debug/health        — schema + row counts + migration state
#   POST /api/extension-error     — extension phones home when a capture
#                                   fails, so we can see failures in the
#                                   server log instead of relying on the
#                                   employee's browser devtools.
#
# Both are intentionally unauthenticated at the route level — during a
# live production incident you don't want auth to be one more thing
# that can fail. They reveal structural info only (no row data).

_APP_START_TS = time.time()


@app.route("/api/debug/health", methods=["GET"])
def debug_health():
    """Report DB shape, row counts, migration state, disk usage, uptime.

    Intended for live incident debugging. Returns 200 even when the DB is
    in bad shape so external monitors can scrape the body for diagnostics.
    """
    out = {
        "server_time": datetime.now().isoformat(),
        "uptime_seconds": int(time.time() - _APP_START_TS),
        "database": "postgres" if is_postgres() else "sqlite",
        "expected_columns": {
            "items": [
                "item_name", "show_id", "org_id", "cost", "preset_name", "sku",
                "notes", "buyer", "order_id", "cancelled_status", "sold_price",
                "sold_timestamp", "viewers", "filename", "pinned_message",
                "image_data",
            ],
        },
        "tables": {},
        "row_counts": {},
        "missing_columns": {},
        "warnings": [],
    }

    try:
        with get_db() as (conn, cursor):
            # Table column lists
            for t in ["orgs", "users", "shows", "items", "presets",
                      "preset_groups", "preset_group_links", "sku_presets",
                      "image_embeddings", "ai_feedback", "recording_sessions"]:
                try:
                    cols = _get_table_columns(cursor, t)
                    out["tables"][t] = cols
                    # Compute missing vs expected
                    expected = out["expected_columns"].get(t)
                    if expected:
                        missing = [c for c in expected if c not in cols]
                        if missing:
                            out["missing_columns"][t] = missing
                            out["warnings"].append(
                                f"table {t} is missing columns: {missing}"
                            )
                except Exception as e:
                    out["tables"][t] = f"<error: {e}>"

            # Row counts (best-effort per-table)
            for t in ["orgs", "users", "shows", "items", "presets",
                      "image_embeddings"]:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {t}")
                    out["row_counts"][t] = int(cursor.fetchone()[0])
                except Exception as e:
                    out["row_counts"][t] = f"<error: {e}>"
    except Exception as e:
        out["warnings"].append(f"db inspection failed: {e}")

    # Disk usage — only meaningful on SQLite / local
    try:
        if not is_postgres() and DB_FILE and Path(DB_FILE).exists():
            out["db_file_bytes"] = Path(DB_FILE).stat().st_size
        captures_bytes = 0
        captures_files = 0
        if CAPTURES_DIR and Path(CAPTURES_DIR).exists():
            for p in Path(CAPTURES_DIR).rglob("*"):
                if p.is_file():
                    captures_bytes += p.stat().st_size
                    captures_files += 1
        out["captures_bytes"] = captures_bytes
        out["captures_files"] = captures_files
    except Exception as e:
        out["warnings"].append(f"disk usage failed: {e}")

    # Extension errors recorded on this server
    try:
        cnt = fetch_one("SELECT COUNT(*) FROM extension_errors")
        out["extension_errors_count"] = int(cnt[0]) if cnt else 0
    except Exception:
        out["extension_errors_count"] = None

    return jsonify(out)


@app.route("/api/extension-error", methods=["POST"])
def extension_error():
    """Accept an error report from the Chrome extension.

    Body JSON:
      context       — short tag, e.g. "upload_failed", "stale_frame"
      details       — free-form string (truncated to 2KB)
      timestamp     — ISO-8601 from the client (optional; server fills if absent)
      dashboard_url — whatever the extension had configured when the error hit
      show_id       — optional int
      item_title    — optional string
      user_agent    — optional string

    We persist it to an `extension_errors` table so /api/debug/health can
    expose the count, and a dedicated endpoint (below) can show recent rows.
    """
    try:
        data = request.get_json(silent=True) or {}
    except Exception:
        data = {}

    context = str(data.get("context") or "")[:80]
    details = str(data.get("details") or "")[:2000]
    client_ts = str(data.get("timestamp") or "")[:64]
    dashboard_url = str(data.get("dashboard_url") or "")[:256]
    show_id_raw = data.get("show_id")
    item_title = str(data.get("item_title") or "")[:256]
    user_agent = str(data.get("user_agent") or request.headers.get("User-Agent", ""))[:256]
    remote_addr = (request.headers.get("X-Forwarded-For") or request.remote_addr or "")[:64]

    try:
        show_id = int(show_id_raw) if show_id_raw not in (None, "") else None
    except (TypeError, ValueError):
        show_id = None

    try:
        execute(
            """INSERT INTO extension_errors
                 (context, details, client_ts, dashboard_url, show_id,
                  item_title, user_agent, remote_addr)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (context, details, client_ts, dashboard_url, show_id,
             item_title, user_agent, remote_addr),
        )
        return jsonify({"success": True})
    except Exception as e:
        # Never fail loudly — the extension already has a local copy of the
        # error in its chrome.storage.local. Just log and ack.
        app.logger.warning(f"extension-error ingest failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 200


@app.route("/api/debug/db-size", methods=["GET"])
def debug_db_size():
    """Structural breakdown of DB storage use. Needed because Render's
    'Storage used' gauge is coarse — we want to know which table and
    which column (toasted BYTEA image_data, typically) is eating space,
    and how much is dead tuples waiting on autovacuum. Auth-exempt so
    it keeps working when the DB is near-full and responses are flaky."""
    out = {"database": "postgres" if is_postgres() else "sqlite"}
    try:
        with get_db() as (conn, cursor):
            if is_postgres():
                cursor.execute("SELECT pg_database_size(current_database())")
                out["database_bytes"] = int(cursor.fetchone()[0])
                cursor.execute("""
                    SELECT
                      relname,
                      pg_total_relation_size(c.oid) AS total_bytes,
                      pg_relation_size(c.oid)       AS heap_bytes,
                      pg_indexes_size(c.oid)        AS index_bytes,
                      pg_total_relation_size(reltoastrelid) AS toast_bytes,
                      n_live_tup, n_dead_tup
                    FROM pg_class c
                    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                    WHERE c.relkind = 'r' AND c.relnamespace IN
                          (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    ORDER BY pg_total_relation_size(c.oid) DESC
                """)
                rows = cursor.fetchall()
                out["tables"] = [{
                    "name": r[0],
                    "total_bytes": int(r[1] or 0),
                    "heap_bytes": int(r[2] or 0),
                    "index_bytes": int(r[3] or 0),
                    "toast_bytes": int(r[4] or 0),
                    "live_tuples": int(r[5] or 0) if r[5] is not None else None,
                    "dead_tuples": int(r[6] or 0) if r[6] is not None else None,
                } for r in rows]
            else:
                import os as _os
                if DB_FILE and _os.path.exists(DB_FILE):
                    out["database_bytes"] = _os.path.getsize(DB_FILE)
                # sqlite page counts per table via dbstat (may not be enabled)
                out["note"] = "SQLite breakdown not implemented; use du on DB file"
    except Exception as e:
        out["error"] = str(e)
    return jsonify(out)


@app.route("/api/debug/extension-errors", methods=["GET"])
def debug_extension_errors():
    """Return the most recent extension errors. Defaults to last 100."""
    try:
        limit = min(int(request.args.get("limit", 100)), 1000)
    except (TypeError, ValueError):
        limit = 100
    try:
        rows = fetch_all(
            """SELECT id, context, details, client_ts, dashboard_url, show_id,
                      item_title, user_agent, remote_addr, created_at
               FROM extension_errors
               ORDER BY id DESC
               LIMIT ?""",
            (limit,),
        )
        return jsonify([
            {
                "id": r[0], "context": r[1], "details": r[2],
                "client_ts": r[3], "dashboard_url": r[4], "show_id": r[5],
                "item_title": r[6], "user_agent": r[7], "remote_addr": r[8],
                "created_at": str(r[9]) if r[9] is not None else None,
            } for r in rows
        ])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


init_db()
fix_csv_file()

if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 8081))  # Use 8081 to avoid conflict with WN companion
    start_auto_update_thread()
    app.run(debug=True, host='0.0.0.0', port=port)
