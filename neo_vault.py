"""
NEO VAULT — Centralized Singleton Database for all AI modes.
Tracks sessions, user interactions, skill proficiency, and usage metrics.
"""

import sqlite3
import datetime
import os
import threading
import secrets
import hashlib
import json
from typing import Optional, Dict, Any
import pyotp
from jose import jwt, JWTError


class NeoVault:
    """
    Singleton database manager. All modules share one connection via NeoVault().
    Database: neo_master.db (absolute path, always reliable).
    """
    _instance = None
    _lock = threading.Lock()
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "neo_web.db")

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(NeoVault, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.conn = sqlite3.connect(self.DB_PATH, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")   # Better concurrent write safety
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._setup()
        self._initialized = True

    # ── Schema ──────────────────────────────────────────────────────────────
    def _setup(self):
        self.        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                game        TEXT,
                mode        TEXT,
                started_at  TEXT,
                ended_at    TEXT,
                duration_s  REAL
            );

            CREATE TABLE IF NOT EXISTS user_interactions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  INTEGER,
                type        TEXT,   -- 'voice' | 'ui'
                input       TEXT,
                response    TEXT,
                ts          TEXT
            );

            CREATE TABLE IF NOT EXISTS user_skills (
                skill_name   TEXT PRIMARY KEY,
                category     TEXT,  -- 'General' | 'Car' | 'Truck'
                rating       REAL    DEFAULT 0.0,
                usage_count  INTEGER DEFAULT 0,
                last_updated TEXT
            );

            CREATE TABLE IF NOT EXISTS usage_metrics (
                day          TEXT PRIMARY KEY,  -- YYYY-MM-DD
                total_time_s REAL    DEFAULT 0.0,
                interactions INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS self_drive_data (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id    INTEGER,
                steer         REAL,
                action        TEXT,
                center_offset REAL,
                ts            TEXT
            );

            CREATE TABLE IF NOT EXISTS assist_data (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id          INTEGER,
                correction_applied  INTEGER,
                steer_delta         REAL,
                ts                  TEXT
            );

            CREATE TABLE IF NOT EXISTS safety_data (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  INTEGER,
                event_type  TEXT,
                severity    TEXT,
                ts          TEXT
            );

            CREATE TABLE IF NOT EXISTS instructor_data (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  INTEGER,
                tip         TEXT,
                steer       REAL,
                speed       REAL,
                ts          TEXT
            );

            CREATE TABLE IF NOT EXISTS performance_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  INTEGER,
                fps         REAL,
                latency_ms  REAL,
                memory_mb   REAL,
                ts          TEXT
            );

            CREATE TABLE IF NOT EXISTS error_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  INTEGER,
                error_type  TEXT,
                message     TEXT,
                ts          TEXT
            );

             CREATE TABLE IF NOT EXISTS settings (
                 key         TEXT PRIMARY KEY,
                 value       TEXT
             );

             CREATE TABLE IF NOT EXISTS audit_log (
                 id          INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp   TEXT,
                 event_type  TEXT,  -- 'login_success', 'login_failure', 'password_reset', etc.
                 username    TEXT,
                 ip_address  TEXT,  -- For future use
                 user_agent  TEXT,  -- For future use
                 details     TEXT   -- JSON string with additional info
             );

             CREATE TABLE IF NOT EXISTS email_queue (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 to_email        TEXT NOT NULL,
                 subject         TEXT NOT NULL,
                 html_body       TEXT,
                 text_body       TEXT,
                 template_name   TEXT,  -- For tracking which template was used
                 template_data   TEXT,  -- JSON data for template rendering
                 priority        INTEGER DEFAULT 1,  -- 1=normal, 2=high, 3=urgent
                 status          TEXT DEFAULT 'pending',  -- 'pending', 'sending', 'sent', 'failed'
                 attempts        INTEGER DEFAULT 0,
                 max_attempts    INTEGER DEFAULT 3,
                 created_at      TEXT,
                 sent_at         TEXT,
                 error_message   TEXT,
                 user_id         INTEGER,  -- Link to user if applicable
                 FOREIGN KEY (user_id) REFERENCES users(id)
             );

             CREATE TABLE IF NOT EXISTS email_templates (
                 name            TEXT PRIMARY KEY,
                 subject         TEXT,
                 html_template   TEXT,
                 text_template   TEXT,
                 created_at      TEXT,
                 updated_at      TEXT
             );

             CREATE TABLE IF NOT EXISTS email_workflows (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 name            TEXT UNIQUE,
                 description     TEXT,
                 trigger_event   TEXT,  -- 'user_registered', 'user_verified', 'password_reset', etc.
                 steps           TEXT,  -- JSON array of steps with delays
                 active          INTEGER DEFAULT 1,
                 created_at      TEXT
             );

             CREATE TABLE IF NOT EXISTS user_sessions (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 user_id         INTEGER,
                 jwt_token       TEXT UNIQUE,
                 device_info     TEXT,  -- JSON with device details
                 ip_address      TEXT,
                 created_at      TEXT,
                 expires_at      TEXT,
                 last_activity   TEXT,
                 active          INTEGER DEFAULT 1,
                 FOREIGN KEY (user_id) REFERENCES users(id)
             );

             CREATE TABLE IF NOT EXISTS trusted_devices (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 user_id         INTEGER,
                 device_hash     TEXT UNIQUE,  -- Hash of device fingerprint
                 device_name     TEXT,
                 trusted_at      TEXT,
                 last_used       TEXT,
                 FOREIGN KEY (user_id) REFERENCES users(id)
             );

             CREATE TABLE IF NOT EXISTS email_analytics (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 email_id        INTEGER,  -- References email_queue.id
                 event_type      TEXT,  -- 'delivered', 'opened', 'clicked', 'bounced', 'unsubscribed'
                 event_data      TEXT,  -- JSON with additional data
                 occurred_at     TEXT,
                 FOREIGN KEY (email_id) REFERENCES email_queue(id)
             );

             CREATE TABLE IF NOT EXISTS users (
                 id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                 username            TEXT UNIQUE,
                 password_hash       TEXT,
                 salt                TEXT,  -- For bcrypt hashing
                 email               TEXT UNIQUE,
                 verified            INTEGER DEFAULT 0,
                 verification_code   TEXT,
                 recovery_code       TEXT,
                 recovery_expiry     TEXT,
                 created_at          TEXT,
                 last_login          TEXT,
                 login_attempts      INTEGER DEFAULT 0,  -- Failed login attempts
                 last_attempt        TEXT,  -- Last failed login timestamp
                 lockout_until       TEXT,  -- Account lockout timestamp
                 twofa_secret        TEXT,  -- TOTP secret
                 jwt_secret          TEXT   -- JWT signing secret
             );

             -- Indexes for optimization
             CREATE INDEX IF NOT EXISTS idx_sessions_game ON sessions(game);
             CREATE INDEX IF NOT EXISTS idx_sessions_mode ON sessions(mode);
             CREATE INDEX IF NOT EXISTS idx_sessions_started_at ON sessions(started_at);
             CREATE INDEX IF NOT EXISTS idx_user_interactions_session_id ON user_interactions(session_id);
             CREATE INDEX IF NOT EXISTS idx_user_interactions_ts ON user_interactions(ts);
             CREATE INDEX IF NOT EXISTS idx_user_skills_category ON user_skills(category);
             CREATE INDEX IF NOT EXISTS idx_self_drive_data_session_id ON self_drive_data(session_id);
             CREATE INDEX IF NOT EXISTS idx_assist_data_session_id ON assist_data(session_id);
             CREATE INDEX IF NOT EXISTS idx_safety_data_session_id ON safety_data(session_id);
             CREATE INDEX IF NOT EXISTS idx_instructor_data_session_id ON instructor_data(session_id);
             CREATE INDEX IF NOT EXISTS idx_performance_logs_session_id ON performance_logs(session_id);
             CREATE INDEX IF NOT EXISTS idx_performance_logs_ts ON performance_logs(ts);
             CREATE INDEX IF NOT EXISTS idx_error_logs_session_id ON error_logs(session_id);
             CREATE INDEX IF NOT EXISTS idx_error_logs_ts ON error_logs(ts);
             CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
             CREATE INDEX IF NOT EXISTS idx_audit_log_event_type ON audit_log(event_type);
             CREATE INDEX IF NOT EXISTS idx_audit_log_username ON audit_log(username);
             CREATE INDEX IF NOT EXISTS idx_email_queue_status ON email_queue(status);
             CREATE INDEX IF NOT EXISTS idx_email_queue_created_at ON email_queue(created_at);
             CREATE INDEX IF NOT EXISTS idx_email_queue_user_id ON email_queue(user_id);
             CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);
             CREATE INDEX IF NOT EXISTS idx_user_sessions_jwt_token ON user_sessions(jwt_token);
             CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at);
             CREATE INDEX IF NOT EXISTS idx_trusted_devices_user_id ON trusted_devices(user_id);
             CREATE INDEX IF NOT EXISTS idx_trusted_devices_device_hash ON trusted_devices(device_hash);
             CREATE INDEX IF NOT EXISTS idx_email_analytics_email_id ON email_analytics(email_id);
             CREATE INDEX IF NOT EXISTS idx_email_analytics_event_type ON email_analytics(event_type);
             CREATE INDEX IF NOT EXISTS idx_email_analytics_occurred_at ON email_analytics(occurred_at);
        """)
        self._init_skills()
        self._migrate_users_table()
        self.conn.commit()

    def _init_skills(self):
        """Seed the skill table with all default proficiency entries."""
        skills = [
            # General
            ("Reaction Time",       "General"),
            ("Command Accuracy",    "General"),
            ("System Engagement",   "General"),
            # Car-Specific
            ("Braking Precision",   "Car"),
            ("Apex Clipping",       "Car"),
            ("Throttle Control",    "Car"),
            ("Drift Mastery",       "Car"),
            # Truck-Specific
            ("Gear Selection",      "Truck"),
            ("Steering Radius",     "Truck"),
            ("Speed Stability",     "Truck"),
        ]
        now = datetime.datetime.now().isoformat()
        for name, cat in skills:
            self.conn.execute(
                "INSERT OR IGNORE INTO user_skills "
                "(skill_name, category, rating, usage_count, last_updated) VALUES (?,?,?,?,?)",
                (name, cat, 0.0, 0, now)
            )

    def _migrate_users_table(self):
        """Migrate users table to add new security columns."""
        cursor = self.conn.cursor()

        # Check existing columns
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]

        # Add new columns if they don't exist
        new_columns = [
            ("salt", "TEXT"),
            ("login_attempts", "INTEGER DEFAULT 0"),
            ("last_attempt", "TEXT"),
            ("lockout_until", "TEXT"),
            ("twofa_secret", "TEXT"),
            ("jwt_secret", "TEXT")
        ]

        for col_name, col_type in new_columns:
            if col_name not in columns:
                try:
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
                    print(f"[VAULT] Added column {col_name} to users table")
                except Exception as e:
                    print(f"[VAULT] Could not add column {col_name}: {e}")

    # ── Session Management ───────────────────────────────────────────────────
    def start_session(self, game: str, mode: str) -> int:
        now = datetime.datetime.now()
        c = self.conn.cursor()
        c.execute(
            "INSERT INTO sessions (game, mode, started_at) VALUES (?,?,?)",
            (game, mode, now.isoformat())
        )
        day = now.strftime("%Y-%m-%d")
        self.conn.execute(
            "INSERT OR IGNORE INTO usage_metrics (day) VALUES (?)", (day,)
        )
        self.conn.commit()
        return c.lastrowid if c.lastrowid is not None else 0

    def end_session(self, session_id: int, duration_s: float):
        now = datetime.datetime.now()
        self.conn.execute(
            "UPDATE sessions SET ended_at=?, duration_s=? WHERE id=?",
            (now.isoformat(), duration_s, session_id)
        )
        day = now.strftime("%Y-%m-%d")
        self.conn.execute(
            "UPDATE usage_metrics SET total_time_s = total_time_s + ? WHERE day = ?",
            (duration_s, day)
        )
        self.conn.commit()

    # ── Interaction Logging ──────────────────────────────────────────────────
    def log_interaction(self, session_id, i_type: str, i_input: str, i_response: str = ""):
        now = datetime.datetime.now()
        self.conn.execute(
            "INSERT INTO user_interactions (session_id, type, input, response, ts) "
            "VALUES (?,?,?,?,?)",
            (session_id, i_type, i_input, i_response, now.isoformat())
        )
        day = now.strftime("%Y-%m-%d")
        self.conn.execute(
            "INSERT OR IGNORE INTO usage_metrics (day) VALUES (?)", (day,)
        )
        self.conn.execute(
            "UPDATE usage_metrics SET interactions = interactions + 1 WHERE day = ?", (day,)
        )
        self.conn.commit()

    # ── Skill Tracking ───────────────────────────────────────────────────────
    def update_skill(self, name: str, rating_delta: float):
        now = datetime.datetime.now().isoformat()
        self.conn.execute(
            """UPDATE user_skills
               SET rating       = MAX(0.0, MIN(10.0, rating + ?)),
                   usage_count  = usage_count + 1,
                   last_updated = ?
               WHERE skill_name = ?""",
            (rating_delta, now, name)
        )
        self.conn.commit()

    def get_skills(self, category: Optional[str] = None):
        """Returns list of (skill_name, category, rating, usage_count, last_updated)."""
        if category:
            return self.conn.execute(
                "SELECT skill_name, category, rating, usage_count, last_updated "
                "FROM user_skills WHERE category = ? ORDER BY skill_name",
                (category,)
            ).fetchall()
        return self.conn.execute(
            "SELECT skill_name, category, rating, usage_count, last_updated "
            "FROM user_skills ORDER BY category, skill_name"
        ).fetchall()

    # ── Mode-Specific Logging ────────────────────────────────────────────────
    def log_self_drive(self, sid: int, steer: float, action: str, offset: float):
        self.conn.execute(
            "INSERT INTO self_drive_data (session_id,steer,action,center_offset,ts) "
            "VALUES (?,?,?,?,?)",
            (sid, steer, action, offset, datetime.datetime.now().isoformat())
        )
        self.conn.commit()

    def log_assist(self, sid: int, correction_applied: bool, steer_delta: float):
        self.conn.execute(
            "INSERT INTO assist_data (session_id,correction_applied,steer_delta,ts) "
            "VALUES (?,?,?,?)",
            (sid, int(correction_applied), steer_delta, datetime.datetime.now().isoformat())
        )
        self.conn.commit()

    def log_safety_event(self, sid: int, event_type: str, severity: str):
        self.conn.execute(
            "INSERT INTO safety_data (session_id,event_type,severity,ts) VALUES (?,?,?,?)",
            (sid, event_type, severity, datetime.datetime.now().isoformat())
        )
        self.conn.commit()

    def log_instructor_tip(self, sid: int, tip: str, steer: float, speed: float):
        self.conn.execute(
            "INSERT INTO instructor_data (session_id,tip,steer,speed,ts) VALUES (?,?,?,?,?)",
            (sid, tip, steer, speed, datetime.datetime.now().isoformat())
        )
        self.conn.commit()

    def log_performance(self, sid: int, fps: float, latency_ms: float, memory_mb: float):
        self.conn.execute(
            "INSERT INTO performance_logs (session_id, fps, latency_ms, memory_mb, ts) VALUES (?,?,?,?,?)",
            (sid, fps, latency_ms, memory_mb, datetime.datetime.now().isoformat())
        )
        self.conn.commit()

    def log_error(self, sid: int, error_type: str, message: str):
        self.conn.execute(
            "INSERT INTO error_logs (session_id, error_type, message, ts) VALUES (?,?,?,?)",
            (sid, error_type, message, datetime.datetime.now().isoformat())
        )
        self.conn.commit()

    # ── Analytics Queries ────────────────────────────────────────────────────
    def get_session_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]

    def get_total_time_hours(self) -> float:
        """Returns total tracked usage time in hours."""
        result = self.conn.execute(
            "SELECT SUM(total_time_s) FROM usage_metrics"
        ).fetchone()[0]
        return round((result or 0.0) / 3600.0, 2)

    def get_last_active(self) -> str:
        """Returns the most recent session start timestamp, or 'Never'."""
        result = self.conn.execute(
            "SELECT started_at FROM sessions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if result:
            try:
                dt = datetime.datetime.fromisoformat(result[0])
                return dt.strftime("%d %b %Y, %H:%M")
            except Exception:
                return result[0]
        return "Never"

    def get_last_sessions(self, limit: int = 10):
        """Returns (game, mode, started_at, duration_s) for recent sessions."""
        return self.conn.execute(
            "SELECT game, mode, started_at, duration_s "
            "FROM sessions ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()

    def get_usage_history(self, days: int = 7):
        """Returns (day, total_time_s, interactions) for the last N days."""
        return self.conn.execute(
            "SELECT day, total_time_s, interactions "
            "FROM usage_metrics ORDER BY day DESC LIMIT ?",
            (days,)
        ).fetchall()

    def get_top_commands(self, limit: int = 10):
        """Returns (input_text, count) for the most frequent voice/UI inputs."""
        return self.conn.execute(
            """SELECT input, COUNT(*) as cnt
               FROM user_interactions
               WHERE type = 'voice'
               GROUP BY input
               ORDER BY cnt DESC
               LIMIT ?""",
            (limit,)
        ).fetchall()

    def get_mode_usage(self):
        """Returns (mode, session_count) for all modes that have been used."""
        return self.conn.execute(
            """SELECT mode, COUNT(*) as cnt
               FROM sessions
               WHERE mode IS NOT NULL
               GROUP BY mode
               ORDER BY cnt DESC"""
        ).fetchall()

    def get_swot_data(self) -> dict:
        """
        Builds a dynamic SWOT analysis from skill ratings.
        Returns dict with keys: strengths, weaknesses, opportunities, threats.
        """
        strengths_raw  = self.conn.execute(
            "SELECT skill_name FROM user_skills WHERE rating >= 7.0 ORDER BY rating DESC"
        ).fetchall()
        weaknesses_raw = self.conn.execute(
            "SELECT skill_name FROM user_skills WHERE rating < 4.0 ORDER BY rating ASC"
        ).fetchall()
        improving_raw  = self.conn.execute(
            "SELECT skill_name FROM user_skills "
            "WHERE rating >= 4.0 AND rating < 7.0 ORDER BY usage_count DESC LIMIT 3"
        ).fetchall()
        low_usage_raw  = self.conn.execute(
            "SELECT skill_name FROM user_skills WHERE usage_count = 0 LIMIT 3"
        ).fetchall()

        strengths     = [s[0] for s in strengths_raw]  or ["Developing — keep training!"]
        weaknesses    = [w[0] for w in weaknesses_raw] or ["None detected yet."]
        opportunities = [i[0] for i in improving_raw]  or ["Complete more sessions to unlock insights."]
        threats       = [t[0] for t in low_usage_raw]  or ["All skills are being actively tracked."]

        return {
            "strengths":     strengths,
            "weaknesses":    weaknesses,
            "opportunities": opportunities,
            "threats":       threats,
        }

    # ── Settings ─────────────────────────────────────────────────────────────
    def get_setting(self, key: str, default=None):
        res = self.conn.execute(
            "SELECT value FROM settings WHERE key=?", (key,)
        ).fetchone()
        return res[0] if res else default

    def set_setting(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)",
            (key, str(value))
        )
        self.conn.commit()

    def cleanup_old_data(self, days: int = 30):
        """Delete data older than specified days to free up space."""
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat()
        # Delete old sessions and related data
        self.conn.execute("DELETE FROM user_interactions WHERE ts < ?", (cutoff,))
        self.conn.execute("DELETE FROM self_drive_data WHERE ts < ?", (cutoff,))
        self.conn.execute("DELETE FROM assist_data WHERE ts < ?", (cutoff,))
        self.conn.execute("DELETE FROM safety_data WHERE ts < ?", (cutoff,))
        self.conn.execute("DELETE FROM instructor_data WHERE ts < ?", (cutoff,))
        self.conn.execute("DELETE FROM performance_logs WHERE ts < ?", (cutoff,))
        self.conn.execute("DELETE FROM error_logs WHERE ts < ?", (cutoff,))
        # Delete sessions older than cutoff
        self.conn.execute("DELETE FROM sessions WHERE started_at < ?", (cutoff,))
        # Vacuum to reclaim space
        self.conn.execute("VACUUM")
        self.conn.commit()

    def get_db_stats(self) -> dict:
        """Get database statistics for monitoring."""
        stats = {}
        tables = ["sessions", "user_interactions", "user_skills", "usage_metrics",
                  "self_drive_data", "assist_data", "safety_data", "instructor_data",
                  "performance_logs", "error_logs", "settings", "users", "audit_log",
                  "email_queue", "email_templates", "email_workflows", "user_sessions", "trusted_devices", "email_analytics"]
        for table in tables:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count
        return stats

    # ── User Management ──────────────────────────────────────────────────────
    def create_user(self, username: str, password: str, email: str) -> str | None:
        """Create a new user and return verification code."""
        import bcrypt
        import secrets
        # Generate salt and hash password with bcrypt
        salt = bcrypt.gensalt()
        password_hash = bcrypt.hashpw(password.encode(), salt).decode()
        verification_code = secrets.token_hex(4).upper()  # 8-char code
        created_at = datetime.datetime.now().isoformat()
        jwt_secret = secrets.token_hex(32)  # 64-char JWT secret
        try:
            self.conn.execute(
                "INSERT INTO users (username, password_hash, salt, email, verification_code, created_at, jwt_secret) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (username, password_hash, salt.decode(), email, verification_code, created_at, jwt_secret)
            )
            self.conn.commit()
            return verification_code
        except sqlite3.IntegrityError:
            return None  # Username or email exists

    def authenticate_user(self, username: str, password: str) -> Dict[str, Any]:
        """Verify username and password with bcrypt, handle migration, and rate limiting."""
        import bcrypt
        import hashlib
        import json

        now = datetime.datetime.now().isoformat()

        # Get user data
        result = self.conn.execute(
            "SELECT id, password_hash, salt, verified, login_attempts, lockout_until FROM users WHERE username = ?",
            (username,)
        ).fetchone()

        if not result:
            self._log_audit("login_failure", username, "user_not_found", {})
            return {"success": False, "error": "User not found"}

        user_id, stored_hash, salt, verified, attempts, lockout_until = result

        # Check if account is locked out
        if lockout_until and datetime.datetime.fromisoformat(lockout_until) > datetime.datetime.now():
            self._log_audit("login_failure", username, "account_locked", {"lockout_until": lockout_until})
            return {"success": False, "error": "Account locked"}

        # Verify password
        password_valid = False
        if salt:  # New bcrypt format
            try:
                password_valid = bcrypt.checkpw(password.encode(), stored_hash.encode())
            except:
                password_valid = False
        else:  # Old SHA256 format - migrate on successful login
            old_hash = hashlib.sha256(password.encode()).hexdigest()
            if old_hash == stored_hash:
                # Migrate to bcrypt
                new_salt = bcrypt.gensalt()
                new_hash = bcrypt.hashpw(password.encode(), new_salt)
                self.conn.execute(
                    "UPDATE users SET password_hash = ?, salt = ? WHERE id = ?",
                    (new_hash.decode(), new_salt.decode(), user_id)
                )
                password_valid = True

        if password_valid and verified:
            # Check if 2FA is enabled
            twofa_secret = self.get_2fa_secret(user_id)
            if twofa_secret:
                # Password correct, but 2FA required
                return {"success": True, "requires_2fa": True, "user_id": user_id}
            else:
                # Full authentication success
                self.conn.execute(
                    "UPDATE users SET last_login = ?, login_attempts = 0, lockout_until = NULL WHERE id = ?",
                    (now, user_id)
                )
                self.conn.commit()
                self._log_audit("login_success", username, "authenticated", {})
                return {"success": True, "requires_2fa": False, "user_id": user_id}
        else:
            # Failed login - increment attempts
            new_attempts = attempts + 1
            lockout = None
            if new_attempts >= 5:  # Lock out after 5 failed attempts
                lockout = (datetime.datetime.now() + datetime.timedelta(minutes=15)).isoformat()
            self.conn.execute(
                "UPDATE users SET login_attempts = ?, last_attempt = ?, lockout_until = ? WHERE id = ?",
                (new_attempts, now, lockout, user_id)
            )
            self.conn.commit()
            self._log_audit("login_failure", username, "invalid_credentials", {"attempts": new_attempts})
            return {"success": False, "error": "Invalid credentials"}

    def user_exists(self, username: str) -> bool:
        """Check if user exists."""
        result = self.conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        return result is not None

    def verify_user(self, username: str, code: str) -> bool:
        """Verify user with code."""
        result = self.conn.execute(
            "SELECT id FROM users WHERE username = ? AND verification_code = ?",
            (username, code)
        ).fetchone()
        if result:
            self.conn.execute("UPDATE users SET verified = 1, verification_code = NULL WHERE username = ?", (username,))
            self.conn.commit()
            return True
        return False

    def generate_recovery_code(self, email: str) -> str | None:
        """Generate recovery code for email."""
        import secrets
        recovery_code = secrets.token_hex(4).upper()
        expiry = (datetime.datetime.now() + datetime.timedelta(hours=1)).isoformat()
        updated = self.conn.execute(
            "UPDATE users SET recovery_code = ?, recovery_expiry = ? WHERE email = ?",
            (recovery_code, expiry, email)
        ).rowcount
        if updated:
            self.conn.commit()
            return recovery_code
        return None

    def verify_recovery_code(self, email: str, code: str) -> bool:
        """Verify recovery code and check expiry."""
        now = datetime.datetime.now().isoformat()
        result = self.conn.execute(
            "SELECT id FROM users WHERE email = ? AND recovery_code = ? AND recovery_expiry > ?",
            (email, code, now)
        ).fetchone()
        if result:
            # Clear code
            self.conn.execute("UPDATE users SET recovery_code = NULL, recovery_expiry = NULL WHERE email = ?", (email,))
            self.conn.commit()
            return True
        return False

    def reset_password(self, email: str, new_password: str) -> bool:
        """Reset password for verified recovery."""
        import bcrypt
        import secrets
        salt = bcrypt.gensalt()
        password_hash = bcrypt.hashpw(new_password.encode(), salt).decode()
        jwt_secret = secrets.token_hex(32)  # Regenerate JWT secret
        updated = self.conn.execute(
            "UPDATE users SET password_hash = ?, salt = ?, jwt_secret = ? WHERE email = ?",
            (password_hash, salt.decode(), jwt_secret, email)
        ).rowcount
        if updated:
            self.conn.commit()
            self._log_audit("password_reset", "", "success", {"email": email})
            return True
        return False

    def get_user_by_email(self, email: str) -> dict | None:
        """Get user info by email."""
        result = self.conn.execute("SELECT id, username, verified FROM users WHERE email = ?", (email,)).fetchone()
        if result:
            return {"id": result[0], "username": result[1], "verified": bool(result[2])}
        return None

    def log_login(self, username: str):
        """Log login event."""
        now = datetime.datetime.now().isoformat()
        self.conn.execute("UPDATE users SET last_login = ? WHERE username = ?", (now, username))
        self.conn.commit()

    def _log_audit(self, event_type: str, username: str, details_type: str, details: dict):
        """Log security audit event."""
        import json
        now = datetime.datetime.now().isoformat()
        details_json = json.dumps(details)
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, event_type, username, details) VALUES (?, ?, ?, ?)",
            (now, event_type, username, details_json)
        )
        self.conn.commit()

    def check_integrity(self) -> bool:
        """Run integrity check on the database."""
        try:
            result = self.conn.execute("PRAGMA integrity_check").fetchone()[0]
            return result == "ok"
        except Exception:
            return False

    def optimize_db(self):
        """Optimize database performance."""
        self.conn.execute("PRAGMA optimize")
        self.conn.execute("VACUUM")
        self.conn.commit()

    # ── Email System ──────────────────────────────────────────────────────────
    def queue_email(self, to_email: str, subject: str, html_body: str = "", text_body: str = "",
                   template_name: str = "", template_data: dict | None = None, priority: int = 1,
                   user_id: int | None = None) -> int | None:
        """Queue an email for sending."""
        import json
        now = datetime.datetime.now().isoformat()
        template_data_json = json.dumps(template_data) if template_data else None
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO email_queue (to_email, subject, html_body, text_body,
                                       template_name, template_data, priority, created_at, user_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (to_email, subject, html_body, text_body, template_name, template_data_json,
             priority, now, user_id)
        )
        self.conn.commit()
        return cursor.lastrowid or 0

    def get_pending_emails(self, limit: int = 10) -> list:
        """Get pending emails ordered by priority and creation time."""
        return self.conn.execute(
            """SELECT id, to_email, subject, html_body, text_body, template_name, template_data,
                      priority, attempts, max_attempts, user_id
               FROM email_queue
               WHERE status = 'pending' AND attempts < max_attempts
               ORDER BY priority DESC, created_at ASC
               LIMIT ?""",
            (limit,)
        ).fetchall()

    def mark_email_sending(self, email_id: int):
        """Mark email as being sent."""
        self.conn.execute("UPDATE email_queue SET status = 'sending' WHERE id = ?", (email_id,))
        self.conn.commit()

    def mark_email_sent(self, email_id: int):
        """Mark email as sent."""
        now = datetime.datetime.now().isoformat()
        self.conn.execute(
            "UPDATE email_queue SET status = 'sent', sent_at = ? WHERE id = ?",
            (now, email_id)
        )
        self.conn.commit()

    def mark_email_failed(self, email_id: int, error: str):
        """Mark email as failed and increment attempts."""
        now = datetime.datetime.now().isoformat()
        self.conn.execute(
            """UPDATE email_queue SET status = 'pending', attempts = attempts + 1,
                                     error_message = ? WHERE id = ?""",
            (error, email_id)
        )
        # If max attempts reached, mark as failed
        self.conn.execute(
            """UPDATE email_queue SET status = 'failed'
               WHERE id = ? AND attempts >= max_attempts""",
            (email_id,)
        )
        self.conn.commit()

    def save_email_template(self, name: str, subject: str, html_template: str, text_template: str = ""):
        """Save or update an email template."""
        import json
        now = datetime.datetime.now().isoformat()
        self.conn.execute(
            """INSERT OR REPLACE INTO email_templates (name, subject, html_template, text_template,
                                                      created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (name, subject, html_template, text_template, now, now)
        )
        self.conn.commit()

    def get_email_template(self, name: str) -> dict | None:
        """Get email template by name."""
        result = self.conn.execute(
            "SELECT name, subject, html_template, text_template FROM email_templates WHERE name = ?",
            (name,)
        ).fetchone()
        if result:
            return {
                "name": result[0],
                "subject": result[1],
                "html_template": result[2],
                "text_template": result[3]
            }
        return None

    def save_email_workflow(self, name: str, description: str, trigger_event: str, steps: list):
        """Save an email workflow."""
        import json
        now = datetime.datetime.now().isoformat()
        steps_json = json.dumps(steps)
        self.conn.execute(
            """INSERT OR REPLACE INTO email_workflows (name, description, trigger_event, steps, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (name, description, trigger_event, steps_json, now)
        )
        self.conn.commit()

    def get_email_workflow(self, trigger_event: str) -> dict | None:
        """Get workflow by trigger event."""
        result = self.conn.execute(
            "SELECT id, name, description, steps FROM email_workflows WHERE trigger_event = ? AND active = 1",
            (trigger_event,)
        ).fetchone()
        if result:
            import json
            return {
                "id": result[0],
                "name": result[1],
                "description": result[2],
                "steps": json.loads(result[3])
            }
        return None

    def trigger_workflow(self, trigger_event: str, user_id: int, context: dict | None = None):
        """Trigger an email workflow."""
        workflow = self.get_email_workflow(trigger_event)
        if not workflow:
            return

        # Get user email
        user = self.conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            return

        email = user[0]
        context = context or {}

        # Process each step
        for step in workflow["steps"]:
            delay_hours = step.get("delay_hours", 0)
            template_name = step["template_name"]
            priority = step.get("priority", 1)

            # Queue email with delay if needed
            if delay_hours > 0:
                # For simplicity, queue immediately but add delay logic in worker
                # In production, you'd use a scheduler
                pass

            self.queue_email(
                to_email=email,
                subject="",  # Will be filled by template
                template_name=template_name,
                template_data=context,
                priority=priority,
                user_id=user_id
            )

    # ── TOTP 2FA ──────────────────────────────────────────────────────────────
    def setup_2fa(self, user_id: int) -> str:
        """Generate TOTP secret for user and return provisioning URI."""
        secret = pyotp.random_base32()
        self.conn.execute("UPDATE users SET twofa_secret = ? WHERE id = ?", (secret, user_id))
        self.conn.commit()
        return secret

    def get_2fa_secret(self, user_id: int) -> str | None:
        """Get user's TOTP secret."""
        result = self.conn.execute("SELECT twofa_secret FROM users WHERE id = ?", (user_id,)).fetchone()
        return result[0] if result and result[0] else None

    def verify_2fa(self, user_id: int, code: str) -> bool:
        """Verify TOTP code."""
        secret = self.get_2fa_secret(user_id)
        if not secret:
            return False
        totp = pyotp.TOTP(secret)
        return totp.verify(code)

    def enable_2fa(self, user_id: int) -> bool:
        """Enable 2FA for user (after verification)."""
        # This is called after setup and verification
        # For now, 2FA is enabled if secret exists
        return self.get_2fa_secret(user_id) is not None

    def disable_2fa(self, user_id: int):
        """Disable 2FA for user."""
        self.conn.execute("UPDATE users SET twofa_secret = NULL WHERE id = ?", (user_id,))
        self.conn.commit()

    # ── JWT Session Management ────────────────────────────────────────────────
    def create_session(self, user_id: int, device_info: Dict[str, Any] = None,
                      ip_address: str = None) -> str:
        """Create a new JWT session."""
        # Generate JWT secret if not exists
        jwt_secret = self.conn.execute("SELECT jwt_secret FROM users WHERE id = ?", (user_id,)).fetchone()
        if not jwt_secret or not jwt_secret[0]:
            jwt_secret = secrets.token_hex(32)
            self.conn.execute("UPDATE users SET jwt_secret = ? WHERE id = ?", (jwt_secret, user_id))
        else:
            jwt_secret = jwt_secret[0]

        # Create JWT payload
        now = datetime.datetime.utcnow()
        expires = now + datetime.timedelta(hours=24)  # 24 hour sessions
        payload = {
            "user_id": user_id,
            "iat": now,
            "exp": expires,
            "iss": "neo"
        }

        token = jwt.encode(payload, jwt_secret, algorithm="HS256")

        # Store session
        device_info_json = json.dumps(device_info) if device_info else None
        self.conn.execute(
            """INSERT INTO user_sessions (user_id, jwt_token, device_info, ip_address,
                                       created_at, expires_at, last_activity)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (user_id, token, device_info_json, ip_address, now.isoformat(),
             expires.isoformat(), now.isoformat())
        )
        self.conn.commit()

        return token

    def validate_session(self, token: str) -> Dict[str, Any] | None:
        """Validate JWT token and return user info."""
        try:
            # Get session from DB
            session = self.conn.execute(
                "SELECT user_id, expires_at, active FROM user_sessions WHERE jwt_token = ?",
                (token,)
            ).fetchone()

            if not session:
                return None

            user_id, expires_at, active = session
            if not active:
                return None

            # Check expiration
            if datetime.datetime.fromisoformat(expires_at) < datetime.datetime.utcnow():
                self.invalidate_session(token)
                return None

            # Get JWT secret
            jwt_secret = self.conn.execute("SELECT jwt_secret FROM users WHERE id = ?", (user_id,)).fetchone()
            if not jwt_secret:
                return None

            # Decode JWT
            payload = jwt.decode(token, jwt_secret[0], algorithms=["HS256"])

            # Update last activity
            now = datetime.datetime.utcnow().isoformat()
            self.conn.execute(
                "UPDATE user_sessions SET last_activity = ? WHERE jwt_token = ?",
                (now, token)
            )
            self.conn.commit()

            return {"user_id": user_id, "username": payload.get("sub")}

        except JWTError:
            return None

    def invalidate_session(self, token: str):
        """Invalidate a session."""
        self.conn.execute("UPDATE user_sessions SET active = 0 WHERE jwt_token = ?", (token,))
        self.conn.commit()

    def get_user_sessions(self, user_id: int) -> list:
        """Get active sessions for user."""
        return self.conn.execute(
            """SELECT id, device_info, ip_address, created_at, last_activity
               FROM user_sessions WHERE user_id = ? AND active = 1
               ORDER BY last_activity DESC""",
            (user_id,)
        ).fetchall()

    # ── Device Tracking ───────────────────────────────────────────────────────
    def get_device_fingerprint(self, device_info: Dict[str, Any]) -> str:
        """Generate device fingerprint hash."""
        fingerprint = f"{device_info.get('os', '')}|{device_info.get('browser', '')}|{device_info.get('user_agent', '')}"
        return hashlib.sha256(fingerprint.encode()).hexdigest()

    def is_trusted_device(self, user_id: int, device_info: Dict[str, Any]) -> bool:
        """Check if device is trusted."""
        device_hash = self.get_device_fingerprint(device_info)
        result = self.conn.execute(
            "SELECT id FROM trusted_devices WHERE user_id = ? AND device_hash = ?",
            (user_id, device_hash)
        ).fetchone()
        return result is not None

    def trust_device(self, user_id: int, device_info: Dict[str, Any], device_name: str = None):
        """Add device to trusted list."""
        device_hash = self.get_device_fingerprint(device_info)
        now = datetime.datetime.utcnow().isoformat()
        self.conn.execute(
            """INSERT OR REPLACE INTO trusted_devices (user_id, device_hash, device_name, trusted_at, last_used)
               VALUES (?, ?, ?, ?, ?)""",
            (user_id, device_hash, device_name or "Unknown Device", now, now)
        )
        self.conn.commit()

    def get_trusted_devices(self, user_id: int) -> list:
        """Get user's trusted devices."""
        return self.conn.execute(
            "SELECT id, device_name, trusted_at, last_used FROM trusted_devices WHERE user_id = ?",
            (user_id,)
        ).fetchall()

    def untrust_device(self, device_id: int, user_id: int):
        """Remove device from trusted list."""
        self.conn.execute(
            "DELETE FROM trusted_devices WHERE id = ? AND user_id = ?",
            (device_id, user_id)
        )
        self.conn.commit()

    # ── Email Analytics ───────────────────────────────────────────────────────
    def track_email_event(self, email_id: int, event_type: str, event_data: Dict[str, Any] = None):
        """Track email events (delivered, opened, clicked, bounced)."""
        now = datetime.datetime.utcnow().isoformat()
        event_data_json = json.dumps(event_data) if event_data else None
        self.conn.execute(
            "INSERT INTO email_analytics (email_id, event_type, event_data, occurred_at) VALUES (?, ?, ?, ?)",
            (email_id, event_type, event_data_json, now)
        )
        self.conn.commit()

    def get_email_stats(self, user_id: int = None) -> Dict[str, Any]:
        """Get email delivery statistics."""
        query = """
            SELECT
                COUNT(CASE WHEN ea.event_type = 'delivered' THEN 1 END) as delivered,
                COUNT(CASE WHEN ea.event_type = 'opened' THEN 1 END) as opened,
                COUNT(CASE WHEN ea.event_type = 'clicked' THEN 1 END) as clicked,
                COUNT(CASE WHEN ea.event_type = 'bounced' THEN 1 END) as bounced,
                COUNT(CASE WHEN ea.event_type = 'unsubscribed' THEN 1 END) as unsubscribed
            FROM email_analytics ea
            JOIN email_queue eq ON ea.email_id = eq.id
        """
        params = []
        if user_id:
            query += " WHERE eq.user_id = ?"
            params.append(user_id)

        result = self.conn.execute(query, params).fetchone()
        return {
            "delivered": result[0],
            "opened": result[1],
            "clicked": result[2],
            "bounced": result[3],
            "unsubscribed": result[4]
        }

    def handle_email_bounce(self, email_id: int, bounce_type: str, bounce_reason: str):
        """Handle email bounce events."""
        # Mark email as bounced
        self.track_email_event(email_id, "bounced", {
            "bounce_type": bounce_type,
            "reason": bounce_reason
        })

        # For hard bounces, consider marking user email as problematic
        if bounce_type == "hard":
            # Get user email
            email = self.conn.execute(
                "SELECT to_email FROM email_queue WHERE id = ?",
                (email_id,)
            ).fetchone()
            if email:
                # Could add a bounced_emails table or flag users
                # For now, just log it
                print(f"[EMAIL] Hard bounce for {email[0]}: {bounce_reason}")

    def unsubscribe_user(self, email: str):
        """Handle user unsubscribe."""
        # Find recent emails to this address and mark as unsubscribed
        recent_emails = self.conn.execute(
            "SELECT id FROM email_queue WHERE to_email = ? AND created_at > ?",
            (email, (datetime.datetime.now() - datetime.timedelta(days=30)).isoformat())
        ).fetchall()

        for email_record in recent_emails:
            self.track_email_event(email_record[0], "unsubscribed")

        # Could also add user to unsubscribe list
        print(f"[EMAIL] User {email} unsubscribed")

    def get_email_performance_stats(self) -> Dict[str, Any]:
        """Get email delivery performance statistics."""
        # Queue stats
        queue_stats = self.conn.execute("""
            SELECT
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
                COUNT(CASE WHEN status = 'sending' THEN 1 END) as sending,
                COUNT(CASE WHEN status = 'sent' THEN 1 END) as sent,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                AVG(CASE WHEN sent_at IS NOT NULL THEN
                    (julianday(sent_at) - julianday(created_at)) * 86400
                END) as avg_delivery_time_seconds
            FROM email_queue
            WHERE created_at > ?
        """, ((datetime.datetime.now() - datetime.timedelta(days=7)).isoformat(),)).fetchone()

        # Failure rate
        total_emails = queue_stats[0] + queue_stats[1] + queue_stats[2] + queue_stats[3]
        failure_rate = (queue_stats[3] / total_emails * 100) if total_emails > 0 else 0

        return {
            "pending": queue_stats[0],
            "sending": queue_stats[1],
            "sent": queue_stats[2],
            "failed": queue_stats[3],
            "failure_rate_percent": round(failure_rate, 2),
            "avg_delivery_time_seconds": round(queue_stats[4] or 0, 2)
        }

    def get_user_skills(self) -> list:
        """Get all user skills with ratings."""
        cursor = self.conn.execute("SELECT skill_name, category, rating, usage_count, last_updated FROM user_skills ORDER BY category, skill_name")
        return [dict(row) for row in cursor.fetchall()]

    def get_recent_sessions(self, user_id: int, limit: int = 10) -> list:
        """Get recent sessions for a user."""
        cursor = self.conn.execute(
            "SELECT id, game, mode, started_at, ended_at, duration_s FROM sessions ORDER BY started_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]
