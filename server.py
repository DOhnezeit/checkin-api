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
from firebase_admin._messaging_utils import UnregisteredError
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.middleware.cors import CORSMiddleware

# Config
DB_PATH = os.environ.get("DB_PATH", "checkin.db")
SERVICE_ACCOUNT = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "serviceAccountKey.json")
API_KEY = os.environ.get("API_KEY", "api-key")
CHECK_INTERVAL_SECONDS = 5  # scheduler runs every 5 seconds
ALARM_REPEAT_INTERVAL_SECONDS = 5  # send alarm notification every 5 seconds
ALARM_TIMEOUT_MINUTES = 30  # stop alarm after 30 minutes if not acknowledged
REMINDER_REPEAT_INTERVAL_SECONDS = 180  # send reminder every 3 minutes (180 seconds)

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
        check_interval REAL DEFAULT 60,
        check_window REAL DEFAULT 15,
        reminder_sent INTEGER DEFAULT 0,
        sleeping INTEGER DEFAULT 0,
        alarm_active INTEGER DEFAULT 0,
        last_alarm_sent INTEGER DEFAULT 0,
        last_reminder_sent INTEGER DEFAULT 0,
        emergency INTEGER DEFAULT 0,
        pulse TEXT DEFAULT NULL,
        blood_pressure TEXT DEFAULT NULL,
        last_health_checkin INTEGER DEFAULT NULL
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
    c.execute("""
    CREATE TABLE IF NOT EXISTS checker_tokens (
        checker_id TEXT PRIMARY KEY,
        token TEXT NOT NULL
    )
    """)
    
    try:
        c.execute("ALTER TABLE checkins ADD COLUMN last_health_checkin INTEGER DEFAULT NULL")
        conn.commit()
        logger.info("Added last_health_checkin column to checkins table")
    except sqlite3.OperationalError:
        # Column already exists
        pass
    
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
    timestamp: Optional[int] = None
    check_interval: Optional[float] = 60
    check_window: Optional[float] = 15
    pulse: Optional[str] = None
    blood_pressure: Optional[str] = None


class SleepRequest(BaseModel):
    checker_id: str


class AcknowledgeAlarmRequest(BaseModel):
    checker_id: str


class EmergencyRequest(BaseModel):
    checker_id: str


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

    ts = now_ms
    drift_sec = None
    if payload.timestamp is not None:
        drift_ms = now_ms - payload.timestamp
        drift_sec = drift_ms / 1000.0
        logger.info(f"Clock drift for {payload.checker_id}: {drift_sec:.2f}s (positive means server ahead)")

    interval = payload.check_interval or 60
    window = payload.check_window or 15

    conn = get_conn()
    c = conn.cursor()

    # Fetch existing check-in if any
    c.execute("SELECT pulse, blood_pressure, last_health_checkin FROM checkins WHERE checker_id = ?", (payload.checker_id,))
    existing = c.fetchone()

    # Determine what values to store in DB
    # Track if health data was actually provided in THIS check-in
    health_data_provided = payload.pulse is not None or payload.blood_pressure is not None
    
    if existing:
        final_pulse = payload.pulse if payload.pulse is not None else existing[0]
        final_blood_pressure = payload.blood_pressure if payload.blood_pressure is not None else existing[1]
        # Update last_health_checkin ONLY if new health data was provided
        final_health_checkin = ts if health_data_provided else existing[2]
    else:
        final_pulse = payload.pulse
        final_blood_pressure = payload.blood_pressure
        final_health_checkin = ts if health_data_provided else None

    # Insert or update check-in: RESET reminder_sent, last_reminder_sent AND emergency flag
    c.execute("""
    INSERT INTO checkins(
        checker_id, last_checkin, missed_notified, reminder_sent, 
        check_interval, check_window, sleeping, alarm_active, last_alarm_sent, 
        last_reminder_sent, emergency, pulse, blood_pressure, last_health_checkin
    )
    VALUES (?, ?, 0, 0, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?)
    ON CONFLICT(checker_id) DO UPDATE SET
        last_checkin=excluded.last_checkin,
        missed_notified=0,
        reminder_sent=0,
        sleeping=0,
        alarm_active=0,
        last_alarm_sent=0,
        last_reminder_sent=0,
        emergency=0,
        check_interval=excluded.check_interval,
        check_window=excluded.check_window,
        pulse=excluded.pulse,
        blood_pressure=excluded.blood_pressure,
        last_health_checkin=excluded.last_health_checkin
    """, (payload.checker_id, ts, interval, window, final_pulse, final_blood_pressure, final_health_checkin))
    conn.commit()

    # Notify watchers
    c.execute("SELECT watcher_id, watcher_token FROM watchers WHERE checker_id = ?", (payload.checker_id,))
    watcher_rows = c.fetchall()
    watcher_targets = [{"token": r[1], "role": "watcher", "id": r[0]} for r in watcher_rows]

    if watcher_targets:
        # Build notification body
        body_text = f"{payload.checker_id} checked in!"

        # Only include health data in the notification if explicitly sent in this check-in
        if payload.pulse is not None or payload.blood_pressure is not None:
            body_text += " "
            if payload.pulse is not None:
                body_text += f"❤️ {payload.pulse}"
            if payload.blood_pressure is not None:
                body_text += f" | 🩸 {payload.blood_pressure}"

        # Data payload
        data_payload = {
            "type": "checkin",
            "checker_id": payload.checker_id,
            "checkin_time": str(ts)
        }
        if payload.pulse is not None:
            data_payload["pulse"] = str(payload.pulse)
        if payload.blood_pressure is not None:
            data_payload["blood_pressure"] = str(payload.blood_pressure)

        # Send FCM notification
        result = send_fcm_to_tokens(
            watcher_targets,
            title=f"Check in at {time.strftime('%H:%M', time.localtime(ts/1000))}",
            body=body_text,
            data=data_payload
        )
        logger.info(f"Sent check-in notification for {payload.checker_id} to watchers: {result}")

    conn.close()
    logger.info(f"Checkin recorded for {payload.checker_id} at {ts}")
    return {"ok": True, "timestamp": ts}


@app.post("/emergency")
def emergency(payload: EmergencyRequest, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    now_ms = int(time.time() * 1000)
    
    conn = get_conn()
    c = conn.cursor()
    
    # Set alarm_active AND emergency flags, but DO NOT set missed_notified
    # (emergency is not a "missed check-in", it's a manual alarm trigger)
    c.execute("""
        UPDATE checkins 
        SET alarm_active = 1, last_alarm_sent = ?, emergency = 1
        WHERE checker_id = ?
    """, (now_ms, payload.checker_id))
    
    # If checker doesn't exist, create entry
    if c.rowcount == 0:
        c.execute("""
            INSERT INTO checkins (checker_id, last_checkin, alarm_active, missed_notified, last_alarm_sent, emergency)
            VALUES (?, ?, 1, 0, ?, 1)
        """, (payload.checker_id, now_ms, 0, now_ms))
    
    conn.commit()
    
    # Send emergency alarm to WATCHERS ONLY
    c.execute("SELECT watcher_id, watcher_token FROM watchers WHERE checker_id = ?", (payload.checker_id,))
    watcher_rows = c.fetchall()
    targets = [{"token": wtoken, "role": "watcher", "id": wid} for wid, wtoken in watcher_rows]
    
    if targets:
        result = send_fcm_to_tokens(
            targets,
            title="🚨 EMERGENCY ALARM!",
            body=f"⚠️ {payload.checker_id} TRIGGERED AN EMERGENCY!",
            data={
                "type": "alarm",
                "checker_id": payload.checker_id,
                "alarm_loop": "true",
                "emergency": "true",
                "checkin_time": str(now_ms)
            }
        )
        logger.info(f"🚨 Sent EMERGENCY alarm for {payload.checker_id} to {len(targets)} watchers: {result}")
    else:
        logger.warning(f"❌ No watcher tokens found for emergency alarm for {payload.checker_id}")
    
    conn.close()
    logger.info(f"🚨 EMERGENCY alarm triggered by {payload.checker_id}")
    return {"ok": True, "timestamp": now_ms}


@app.post("/acknowledge_alarm")
def acknowledge_alarm(payload: AcknowledgeAlarmRequest, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE checkins 
        SET alarm_active = 0, last_alarm_sent = 0, emergency = 0 
        WHERE checker_id = ?
    """, (payload.checker_id,))
    conn.commit()
    conn.close()
    logger.info(f"Alarm acknowledged for {payload.checker_id}")
    return {"ok": True}


@app.post("/sleep")
def set_sleep(payload: SleepRequest, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    c = conn.cursor()

    # Check current state before updating
    c.execute("""
        SELECT sleeping, alarm_active, missed_notified, reminder_sent 
        FROM checkins 
        WHERE checker_id = ?
    """, (payload.checker_id,))
    row = c.fetchone()

    if row:
        sleeping, alarm_active, missed_notified, reminder_sent = row
        logger.info(f"🛏️ Sleep requested for {payload.checker_id}. Current state: sleeping={sleeping}, alarm_active={alarm_active}, missed_notified={missed_notified}, reminder_sent={reminder_sent}")

        if sleeping == 1:
            conn.close()
            logger.info(f"{payload.checker_id} is already marked as sleeping: skipping duplicate notification.")
            return {"ok": True, "message": "Already sleeping"}

        if alarm_active == 1:
            logger.info(f"🔕 Stopping active alarm for {payload.checker_id}")

    ts = int(time.time() * 1000)

    # Update to sleeping state and clear ALL alarm/reminder/emergency flags
    c.execute("""
        INSERT INTO checkins (checker_id, last_checkin, sleeping, reminder_sent, missed_notified, alarm_active, last_alarm_sent, last_reminder_sent, emergency)
        VALUES (?, ?, 1, 0, 0, 0, 0, 0, 0)
        ON CONFLICT(checker_id) DO UPDATE SET 
            last_checkin = ?,
            sleeping = 1,
            reminder_sent = 0,
            missed_notified = 0,
            alarm_active = 0,
            last_alarm_sent = 0,
            last_reminder_sent = 0,
            emergency = 0
    """, (payload.checker_id, ts, ts))
    conn.commit()

    # Verify the update worked
    c.execute("""
        SELECT sleeping, alarm_active, missed_notified 
        FROM checkins 
        WHERE checker_id = ?
    """, (payload.checker_id,))
    verify_row = c.fetchone()
    if verify_row:
        logger.info(f"✅ Sleep state updated. New state: sleeping={verify_row[0]}, alarm_active={verify_row[1]}, missed_notified={verify_row[2]}")

    # Send sleep notification to watchers
    c.execute("SELECT watcher_token FROM watchers WHERE checker_id = ?", (payload.checker_id,))
    tokens_from_db = c.fetchall()

    if tokens_from_db:
        tokens = [
            {"token": r[0], "role": "watcher", "id": f"Watcher-{i}"}
            for i, r in enumerate(tokens_from_db)
        ]

        send_fcm_to_tokens(
            tokens,
            title="Checker asleep 💤",
            body=f"{payload.checker_id} has gone to sleep, check-ins paused until morning.",
            data={"type": "sleep",
                  "checker_id": payload.checker_id,
                  "checkin_time": str(ts)
            }
        )
        logger.info(f"📢 Sent sleep notification to {len(tokens)} watchers")

    conn.close()
    logger.info(f"😴 {payload.checker_id} is now marked as sleeping.")
    return {"ok": True}


@app.get("/status/{checker_id}")
def status(checker_id: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT last_checkin, missed_notified, check_interval, check_window, sleeping, alarm_active, pulse, blood_pressure, emergency, last_health_checkin FROM checkins WHERE checker_id = ?", (checker_id,))
    row = c.fetchone()
    c.execute("SELECT watcher_id FROM watchers WHERE checker_id = ?", (checker_id,))
    watchers = [r[0] for r in c.fetchall()]
    conn.close()

    if not row:
        return {
            "checker_id": checker_id,
            "last_checkin": None,
            "missed_notified": None,
            "check_interval": 60,
            "check_window": 15,
            "sleeping": False,
            "watchers": watchers,
            "alarm_active": False,
            "pulse": None,
            "blood_pressure": None,
            "emergency": False,
            "last_health_checkin": None
        }

    last_checkin, missed_notified, check_interval, check_window, sleeping, alarm_active, pulse, blood_pressure, emergency, last_health_checkin = row

    return {
        "checker_id": checker_id,
        "last_checkin": last_checkin,
        "missed_notified": bool(missed_notified),
        "check_interval": check_interval,
        "check_window": check_window,
        "sleeping": bool(sleeping),
        "alarm_active": bool(alarm_active),
        "watchers": watchers,
        "pulse": pulse,
        "blood_pressure": blood_pressure,
        "emergency": bool(emergency),
        "last_health_checkin": last_health_checkin
    }

@app.delete("/unregister_checker/{checker_id}")
def unregister_checker(checker_id: str, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM checker_tokens WHERE checker_id = ?", (checker_id,))
    c.execute("DELETE FROM checkins WHERE checker_id = ?", (checker_id,))
    c.execute("DELETE FROM watchers WHERE checker_id = ?", (checker_id,))
    conn.commit()
    conn.close()
    logger.info(f"[DELETION] Unregistered checker token for {checker_id}")
    return {"ok": True}


@app.delete("/unregister_watcher/{checker_id}/{watcher_id}")
def unregister_watcher(checker_id: str, watcher_id: str, x_api_key: str = Header(None)):
    require_api_key(x_api_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM watchers WHERE checker_id = ? AND watcher_id = ?", (checker_id, watcher_id))
    conn.commit()
    conn.close()
    logger.info(f"Unregistered watcher {watcher_id} from {checker_id}")
    return {"ok": True}


# FCM helper
def send_fcm_to_tokens(targets: List[dict], title: str, body: str, data: dict = None):
    """
    targets: list of {"token": "<fcm token>", "role": "checker" or "watcher", "id": "<checker_id or watcher_id>"}
    Sends FCM messages and logs in high detail which token receives what and the result.
    """
    if not targets:
        logger.info("send_fcm_to_tokens called with no targets")
        return {"success": 0, "failure": 0, "per_target": []}

    success = 0
    failure = 0
    per_target = []

    notification_type = (data or {}).get("type", "checkin")
    channel_id = {
        "alarm": "checkin_alarms_v2",
        "reminder": "checkin_reminders_v2",
        "checkin": "checkin_notifications_v2",
        "sleep": "checkin_notifications_v2"
    }.get(notification_type, "checkin_notifications_v2")

    logger.info(f"Preparing to send '{notification_type}' notification to {len(targets)} target(s): title='{title}', body='{body}', channel='{channel_id}', data={data}")

    for t in targets:
        token = t.get("token")
        role = t.get("role", "unknown")
        local_id = t.get("id", "<unknown>")

        # defensive: ensure token exists
        if not token:
            logger.warning(f"Skipping target with missing token: role={role} id={local_id}")
            per_target.append({"token": None, "role": role, "id": local_id, "status": "skipped", "error": "no-token"})
            failure += 1
            continue

        # Build message data (force string values) - INCLUDE title and body in data
        message_data = {
            "title": title,
            "body": body,
            **{k: str(v) for k, v in (data or {}).items()}
        }

        # Log the attempt details before sending (token truncated)
        token_preview = token[-20:] if len(token) > 20 else token
        logger.info(f"FCM SEND ATTEMPT -> token=...{token_preview}, role={role}, id={local_id}, type={notification_type}, title='{title}', body='{body}', data={message_data}, channel_id={channel_id}")

        try:
            message = messaging.Message(
                data=message_data,
                android=messaging.AndroidConfig(
                    priority="high",
                ),
                token=token
            )
            logger.info(f"📤 Sending data-only message: {list(message_data.keys())}")
            response = messaging.send(message)
            logger.info(f"FCM SEND SUCCESS -> token=...{token_preview}, role={role}, id={local_id}, response={response}")
            per_target.append({"token": token, "role": role, "id": local_id, "status": "sent", "response": str(response)})
            success += 1

        except UnregisteredError:
            # Token is invalid -> remove from DB and log
            delete_token_from_db(token)
            logger.warning(f"Deleted invalid FCM token: {token}")
            per_target.append({"token": token, "role": role, "id": local_id, "status": "failed", "error": "Unregistered token"})
            failure += 1

        except Exception as e:
            logger.exception(f"FCM SEND FAILED -> token=...{token_preview}, role={role}, id={local_id}, error={e}")
            per_target.append({"token": token, "role": role, "id": local_id, "status": "failed", "error": str(e)})
            failure += 1

    logger.info(f"FCM send summary: success={success} failure={failure} on channel={channel_id}")
    return {"success": success, "failure": failure, "per_target": per_target}


def delete_token_from_db(token: str):
    """Remove a checker or watcher token from the database if it becomes invalid."""
    conn = get_conn()
    c = conn.cursor()
    # Remove from checker_tokens
    c.execute("DELETE FROM checker_tokens WHERE token = ?", (token,))
    # Remove from watchers
    c.execute("DELETE FROM watchers WHERE watcher_token = ?", (token,))
    conn.commit()
    conn.close()
    logger.info(f"Deleted token from DB: {token}")


# Background job
def check_for_missed():
    try:
        now_ms = int(time.time() * 1000)
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT checker_id, last_checkin, missed_notified, reminder_sent, 
                   check_interval, check_window, sleeping, alarm_active, last_alarm_sent, last_reminder_sent, emergency 
            FROM checkins
        """)
        rows = c.fetchall()

        for checker_id, last_checkin, missed_notified, reminder_sent, check_interval, check_window, sleeping, alarm_active, last_alarm_sent, last_reminder_sent, emergency in rows:
            # Handle EMERGENCY alarms: these repeat every 5 seconds regardless of sleeping state
            if emergency == 1 and alarm_active == 1:
                time_since_last_alarm = now_ms - (last_alarm_sent or 0)
                
                # Check if alarm has timed out
                alarm_timeout_ms = ALARM_TIMEOUT_MINUTES * 60_000
                if last_alarm_sent and (now_ms - last_alarm_sent) >= alarm_timeout_ms:
                    logger.info(f"⏰ Emergency alarm timed out for {checker_id} after {ALARM_TIMEOUT_MINUTES} minutes")
                    c.execute("""
                        UPDATE checkins 
                        SET alarm_active = 0, last_alarm_sent = 0, emergency = 0, missed_notified = 0
                        WHERE checker_id = ?
                    """, (checker_id,))
                    conn.commit()
                    continue
                
                # Repeat emergency alarm every 5 seconds
                if time_since_last_alarm >= ALARM_REPEAT_INTERVAL_SECONDS * 1000:
                    logger.info(f"🚨 Repeating EMERGENCY alarm for {checker_id} (time since last: {time_since_last_alarm}ms)")
                    
                    c2 = conn.cursor()
                    c2.execute("SELECT watcher_id, watcher_token FROM watchers WHERE checker_id = ?", (checker_id,))
                    watcher_rows = c2.fetchall()
                    targets = [{"token": wtoken, "role": "watcher", "id": wid} for wid, wtoken in watcher_rows]
                    
                    if targets:
                        # Use timestamp as unique notification ID to force sound replay
                        result = send_fcm_to_tokens(
                            targets,
                            title="🚨 EMERGENCY ALARM!",
                            body=f"⚠️ {checker_id} TRIGGERED AN EMERGENCY!",
                            data={
                                "type": "alarm",
                                "checker_id": checker_id,
                                "alarm_loop": "true",
                                "emergency": "true",
                                "checkin_time": str(last_checkin or now_ms)
                            }
                        )
                        logger.info(f"📨 Sent EMERGENCY alarm to {len(targets)} watchers for {checker_id}: {result}")
                        
                        c.execute("""
                            UPDATE checkins 
                            SET last_alarm_sent = ? 
                            WHERE checker_id = ?
                        """, (now_ms, checker_id))
                        conn.commit()
                    else:
                        logger.warning(f"❌ No watcher tokens found for emergency alarm for {checker_id}")
                
                # Skip normal check-in logic for emergency cases
                continue
            
            # Normal check-in logic (skip if sleeping)
            if sleeping == 1:
                if alarm_active == 1:
                    logger.warning(f"😴 {checker_id} is sleeping but alarm_active=1 (this shouldn't happen!)")
                logger.debug(f"😴 {checker_id} is sleeping — skipping all checks")
                continue

            last_checkin = last_checkin or 0
            if last_checkin == 0:
                logger.debug(f"{checker_id} has no check-in yet — skipping")
                continue

            interval_ms = (check_interval or 60) * 60_000
            window_ms = (check_window or 15) * 60_000
            elapsed = now_ms - last_checkin

            logger.debug(f"Checking {checker_id}: elapsed={elapsed}ms, interval={interval_ms}ms, window={window_ms}ms, reminder_sent={reminder_sent}, missed_notified={missed_notified}, alarm_active={alarm_active}")

            # Reminder window: from interval_ms to (interval_ms + window_ms)
            reminder_start = interval_ms
            reminder_end = interval_ms + window_ms
            
            if elapsed >= reminder_start and elapsed < reminder_end:
                # Check if we should send a reminder (first time or every 3 minutes)
                should_send_reminder = False
                
                if reminder_sent == 0:
                    # First reminder - send immediately
                    should_send_reminder = True
                    logger.info(f"📢 {checker_id} entered reminder window, sending FIRST reminder")
                else:
                    # Check if 3 minutes have passed since last reminder
                    time_since_last_reminder = now_ms - (last_reminder_sent or 0)
                    if time_since_last_reminder >= REMINDER_REPEAT_INTERVAL_SECONDS * 1000:
                        should_send_reminder = True
                        logger.info(f"📢 {checker_id} sending REPEATED reminder (time since last: {time_since_last_reminder}ms)")
                    else:
                        logger.debug(f"⏳ Not repeating reminder yet for {checker_id} (only {time_since_last_reminder}ms since last)")
                
                if should_send_reminder:
                    c2 = conn.cursor()
                    c2.execute("SELECT token FROM checker_tokens WHERE checker_id = ?", (checker_id,))
                    checker_row = c2.fetchone()
                    if checker_row:
                        checker_targets = [{"token": checker_row[0], "role": "checker", "id": checker_id}]
                        result = send_fcm_to_tokens(
                            checker_targets,
                            title="Time to check in!",
                            body="Please check in now",
                            data={"type": "reminder", "checker_id": checker_id}
                        )
                        logger.info(f"✅ Reminder send result for checker {checker_id}: {result}")
                        
                        # Update reminder flags
                        if reminder_sent == 0:
                            c.execute("UPDATE checkins SET reminder_sent = 1, last_reminder_sent = ? WHERE checker_id = ?", (now_ms, checker_id))
                        else:
                            c.execute("UPDATE checkins SET last_reminder_sent = ? WHERE checker_id = ?", (now_ms, checker_id))
                        conn.commit()
                    else:
                        logger.warning(f"❌ No checker token found for {checker_id}")

            # Missed check-in: after (interval_ms + window_ms)
            if elapsed >= interval_ms + window_ms:
                logger.debug(f"🚨 {checker_id} missed check-in (elapsed={elapsed}ms > {interval_ms + window_ms}ms)")
                
                # Check if alarm has timed out
                alarm_timeout_ms = ALARM_TIMEOUT_MINUTES * 60_000
                if alarm_active == 1 and last_alarm_sent and (now_ms - last_alarm_sent) >= alarm_timeout_ms:
                    logger.info(f"⏰ Alarm timed out for {checker_id} after {ALARM_TIMEOUT_MINUTES} minutes")
                    c.execute("""
                        UPDATE checkins 
                        SET alarm_active = 0, last_alarm_sent = 0, missed_notified = 0
                        WHERE checker_id = ?
                    """, (checker_id,))
                    conn.commit()
                    continue

                # Determine if we should send an alarm notification
                should_send_alarm = False
                
                if missed_notified == 0:
                    # First alarm - always send
                    should_send_alarm = True
                    logger.info(f"🔔 Sending INITIAL alarm for {checker_id}")
                elif alarm_active == 1:
                    # Alarm is active - check if enough time has passed since last alarm
                    time_since_last_alarm = now_ms - (last_alarm_sent or 0)
                    if time_since_last_alarm >= ALARM_REPEAT_INTERVAL_SECONDS * 1000:
                        should_send_alarm = True
                        logger.info(f"🔔 Sending REPEATED alarm for {checker_id} (time since last: {time_since_last_alarm}ms)")
                    else:
                        logger.debug(f"⏳ Not repeating alarm yet for {checker_id} (only {time_since_last_alarm}ms since last)")

                # Send alarm notification if needed (to both checker and watchers for missed check-ins)
                if should_send_alarm:
                    targets = []
                    c2 = conn.cursor()
                    c2.execute("SELECT token FROM checker_tokens WHERE checker_id = ?", (checker_id,))
                    checker_row = c2.fetchone()
                    if checker_row:
                        targets.append({"token": checker_row[0], "role": "checker", "id": checker_id})
                        logger.debug(f"Found checker token for {checker_id}")

                    c2.execute("SELECT watcher_id, watcher_token FROM watchers WHERE checker_id = ?", (checker_id,))
                    watcher_rows = c2.fetchall()
                    for wid, wtoken in watcher_rows:
                        targets.append({"token": wtoken, "role": "watcher", "id": wid})
                    logger.debug(f"Found {len(watcher_rows)} watcher tokens for {checker_id}")

                    if targets:
                        result = send_fcm_to_tokens(
                            targets,
                            title="⚠️ CHECK-IN MISSED!",
                            body=f"{checker_id} missed their check-in! Tap to acknowledge.",
                            data={
                                "type": "alarm",
                                "checker_id": checker_id,
                                "alarm_loop": "true",
                                "checkin_time": str(last_checkin)
                            }
                        )
                        logger.info(f"📨 Sent alarm to {len(targets)} targets for {checker_id}: {result}")

                        # Update DB
                        if missed_notified == 0:
                            c.execute("""
                                UPDATE checkins 
                                SET missed_notified = 1, alarm_active = 1, last_alarm_sent = ? 
                                WHERE checker_id = ?
                            """, (now_ms, checker_id))
                            logger.info(f"✅ Alarm activated for {checker_id}")
                        else:
                            c.execute("""
                                UPDATE checkins 
                                SET last_alarm_sent = ? 
                                WHERE checker_id = ?
                            """, (now_ms, checker_id))
                            logger.info(f"✅ Alarm repeated for {checker_id}")

                        conn.commit()
                    else:
                        logger.warning(f"❌ No tokens found for alarm for {checker_id}")

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