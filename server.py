from dotenv import load_dotenv
load_dotenv()

import os
import time
import sqlite3
import logging
from typing import List, Optional
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import firebase_admin
from firebase_admin import credentials, messaging
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.middleware.cors import CORSMiddleware

# Config
DB_PATH = os.environ.get("DB_PATH", "checkin.db")
SERVICE_ACCOUNT = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "serviceAccountKey.json")
API_KEY = os.environ.get("API_KEY", "api-key")
CHECK_INTERVAL_SECONDS = 5  # scheduler runs every 5 seconds for testing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("checkin_api")

app = FastAPI(title="Checkin API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Firebase Admin
if not os.path.exists(SERVICE_ACCOUNT):
    raise RuntimeError(f"service account JSON not found at {SERVICE_ACCOUNT}")
cred = credentials.Certificate(SERVICE_ACCOUNT)
firebase_admin.initialize_app(cred)

# DB helpers
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS checkins (
        checker_id TEXT PRIMARY KEY,
        last_checkin INTEGER DEFAULT 0,
        missed_notified INTEGER DEFAULT 0,
        check_interval REAL DEFAULT 1,
        check_window REAL DEFAULT 0.5
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS watchers (
        checker_id TEXT,
        watcher_id TEXT,
        watcher_token TEXT,
        PRIMARY KEY (checker_id, watcher_id)
    )
    """)
    # ADD THIS:
    c.execute("""
    CREATE TABLE IF NOT EXISTS checker_tokens (
        checker_id TEXT PRIMARY KEY,
        token TEXT NOT NULL
    )
    """)
    conn.commit()
    conn.close()

init_db()

# Models
class RegisterWatcher(BaseModel):
    checker_id: str
    watcher_id: str
    watcher_token: str

class RegisterChecker(BaseModel):
    checker_id: str
    checker_token: str

class CheckinRequest(BaseModel):
    checker_id: str
    timestamp: Optional[int] = None  # epoch ms; server will set if missing
    check_interval: Optional[float] = 1  # in minutes (float for testing)
    check_window: Optional[float] = 0.5  # in minutes

def require_api_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# Endpoints
@app.post("/register_watcher")
def register_watcher(payload: RegisterWatcher, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO watchers (checker_id, watcher_id, watcher_token) VALUES (?, ?, ?)",
        (payload.checker_id, payload.watcher_id, payload.watcher_token)
    )
    conn.commit()
    conn.close()
    logger.info(f"Registered watcher {payload.watcher_id} for {payload.checker_id}")
    return {"ok": True}

@app.post("/register_checker")
def register_checker(payload: RegisterChecker, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO checker_tokens (checker_id, token) VALUES (?, ?)",
        (payload.checker_id, payload.checker_token)
    )
    conn.commit()
    conn.close()
    logger.info(f"Registered checker token for {payload.checker_id}")
    return {"ok": True}

@app.post("/checkin")
def checkin(payload: CheckinRequest, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    now_ms = int(time.time() * 1000)
    ts = payload.timestamp if payload.timestamp is not None else now_ms
    interval = payload.check_interval or 1
    window = payload.check_window or 0.5

    conn = get_conn()
    c = conn.cursor()
    c.execute("""
       INSERT INTO checkins(checker_id, last_checkin, missed_notified, check_interval, check_window)
       VALUES (?, ?, 0, ?, ?)
       ON CONFLICT(checker_id) DO UPDATE SET
           last_checkin=excluded.last_checkin,
           missed_notified=0,
           check_interval=excluded.check_interval,
           check_window=excluded.check_window
    """, (payload.checker_id, ts, interval, window))
    conn.commit()

    # Notify watchers that checker successfully checked in
    c.execute("SELECT watcher_token FROM watchers WHERE checker_id = ?", (payload.checker_id,))
    tokens = [r[0] for r in c.fetchall()]
    if tokens:
        messaging.send_multicast(
            messaging.MulticastMessage(
                notification=messaging.Notification(
                    title="Check-in successful",
                    body=f"{payload.checker_id} checked in!"
                ),
                tokens=tokens,
                data={"type": "checkin", "checker_id": payload.checker_id}
            )
        )
        logger.info(f"Sent check-in success notification for {payload.checker_id}")

    conn.close()
    logger.info(f"Checkin recorded for {payload.checker_id} at {ts}")
    return {"ok": True, "timestamp": ts}

@app.get("/status/{checker_id}")
def status(checker_id: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT last_checkin, missed_notified, check_interval, check_window FROM checkins WHERE checker_id = ?", (checker_id,))
    row = c.fetchone()
    c.execute("SELECT watcher_id FROM watchers WHERE checker_id = ?", (checker_id,))
    watchers = [r[0] for r in c.fetchall()]
    conn.close()
    if not row:
        return {"checker_id": checker_id, "last_checkin": None, "missed_notified": None, "check_interval": 1, "check_window": 0.5, "watchers": watchers}
    return {
        "checker_id": checker_id,
        "last_checkin": row[0],
        "missed_notified": bool(row[1]),
        "check_interval": row[2],
        "check_window": row[3],
        "watchers": watchers
    }

# FCM helper
def send_fcm_to_tokens(tokens: List[str], title: str, body: str, data: dict = None):
    if not tokens:
        return {"success": 0, "failure": 0}
    message = messaging.MulticastMessage(
        notification=messaging.Notification(title=title, body=body),
        data={k: str(v) for k, v in (data or {}).items()},
        tokens=tokens
    )
    response = messaging.send_multicast(message)
    logger.info(f"FCM send result: success={response.success_count} fail={response.failure_count}")
    return {"success": response.success_count, "failure": response.failure_count}

# Background job: check reminders and missed check-ins
def check_for_missed():
    try:
        now_ms = int(time.time() * 1000)
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT checker_id, last_checkin, missed_notified, check_interval, check_window FROM checkins")
        rows = c.fetchall()

        for checker_id, last_checkin, missed_notified, check_interval, check_window in rows:
            last_checkin = last_checkin or 0
            if last_checkin == 0:
                continue

            interval_ms = (check_interval or 1) * 60_000
            window_ms = (check_window or 0.5) * 60_000
            elapsed = now_ms - last_checkin

            # --- Reminder to CHECKER (not watchers) ---
            if elapsed >= interval_ms and elapsed < interval_ms + window_ms:
                # Get checker's token
                c2 = conn.cursor()
                c2.execute("SELECT token FROM checker_tokens WHERE checker_id = ?", (checker_id,))
                checker_row = c2.fetchone()
                if checker_row:
                    checker_token = checker_row[0]
                    send_fcm_to_tokens(
                        [checker_token],
                        title="Time to check in!",
                        body=f"Please check in now. You have {check_window} minutes.",
                        data={"type": "reminder", "checker_id": checker_id}
                    )
                    logger.info(f"Reminder sent to checker {checker_id}")

            # --- Missed check-in (notify BOTH checker and watchers) ---
            if elapsed >= interval_ms + window_ms and missed_notified == 0:
                # Get all tokens (checker + watchers)
                all_tokens = []
                
                # Get checker token
                c2 = conn.cursor()
                c2.execute("SELECT token FROM checker_tokens WHERE checker_id = ?", (checker_id,))
                checker_row = c2.fetchone()
                if checker_row:
                    all_tokens.append(checker_row[0])
                
                # Get watcher tokens
                c2.execute("SELECT watcher_token FROM watchers WHERE checker_id = ?", (checker_id,))
                all_tokens.extend([r[0] for r in c2.fetchall()])
                
                if all_tokens:
                    send_fcm_to_tokens(
                        all_tokens,
                        title="Check-in missed!",
                        body=f"{checker_id} missed their check-in!",
                        data={"type": "missed", "checker_id": checker_id}
                    )
                    c.execute("UPDATE checkins SET missed_notified = 1 WHERE checker_id = ?", (checker_id,))
                    conn.commit()
                    logger.info(f"Missed notification sent for {checker_id}")

        conn.close()
    except Exception as e:
        logger.exception("Error in check_for_missed job: %s", e)

scheduler = BackgroundScheduler()
scheduler.add_job(check_for_missed, 'interval', seconds=CHECK_INTERVAL_SECONDS)

@app.on_event("startup")
def startup_event():
    logger.info("Starting scheduler...")
    scheduler.start()

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Shutting down scheduler...")
    scheduler.shutdown()
