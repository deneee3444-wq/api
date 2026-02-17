"""
Database Module for API
Supports both SQLite (local) and PostgreSQL (production).
"""
import os
import json
import time
import threading
from datetime import datetime

DATABASE_URL = "postgresql://db_mdj5_user:LgPHY1oCy66PW7W2Q0NdBSwH7UDo5vru@dpg-d66v0sjnv86c73dc22fg-a.oregon-postgres.render.com/db_mdj5"

import psycopg2
from psycopg2.extras import RealDictCursor
DB_TYPE = 'postgresql'
print("Using PostgreSQL database")

db_lock = threading.Lock()

DB_CONNECT_RETRIES = 3
DB_CONNECT_WAIT    = 10  # saniye


def get_connection():
    """DB bağlantısı döndürür. Başarısız olursa 3 kez 10'ar saniye bekleyerek dener."""
    last_err = None
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            if DB_TYPE == 'postgresql':
                return psycopg2.connect(DATABASE_URL, connect_timeout=10)
            else:
                import sqlite3
                conn = sqlite3.connect("api.db", check_same_thread=False)
                conn.row_factory = sqlite3.Row
                return conn
        except Exception as e:
            last_err = e
            print(f"[DB] Bağlantı denemesi {attempt}/{DB_CONNECT_RETRIES} başarısız: {e}")
            if attempt < DB_CONNECT_RETRIES:
                time.sleep(DB_CONNECT_WAIT)
    raise last_err


def init_db():
    """Tabloları oluşturur, eksik kolonları ekler."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()

        if DB_TYPE == 'postgresql':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    id SERIAL PRIMARY KEY,
                    key TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
                    email TEXT NOT NULL,
                    password TEXT NOT NULL,
                    used INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(api_key_id, email)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
                    task_id TEXT UNIQUE NOT NULL,
                    status TEXT DEFAULT 'pending',
                    result_url TEXT,
                    logs TEXT DEFAULT '[]',
                    mode TEXT,
                    external_task_id TEXT,
                    token TEXT,
                    account_email TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Mevcut tabloya eksik kolonları güvenle ekle
            for col in ['external_task_id', 'token', 'account_email']:
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='tasks' AND column_name=%s",
                    (col,)
                )
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE tasks ADD COLUMN {col} TEXT")
                    print(f"[DB] tasks tablosuna '{col}' kolonu eklendi.")

        else:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
                    email TEXT NOT NULL,
                    password TEXT NOT NULL,
                    used INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(api_key_id, email)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
                    task_id TEXT UNIQUE NOT NULL,
                    status TEXT DEFAULT 'pending',
                    result_url TEXT,
                    logs TEXT DEFAULT '[]',
                    mode TEXT,
                    external_task_id TEXT,
                    token TEXT,
                    account_email TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

        conn.commit()
        conn.close()


def cleanup_orphan_tasks():
    """
    Startup temizliği:
    - external_task_id NULL olan running/pending taskler: dışarıya hiç istek gitmemiş.
    - Bunları failed yap, task'a kayıtlı account_email'i (varsa) used=0 yap.
    - account_email NULL ise hesap zaten kullanılmamış, sadece task failed olur.
    """
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor) if DB_TYPE == 'postgresql' else conn.cursor()

        if DB_TYPE == 'postgresql':
            cursor.execute("""
                SELECT task_id, api_key_id, account_email
                FROM tasks
                WHERE status IN ('running', 'pending') AND external_task_id IS NULL
            """)
            orphans = [dict(r) for r in cursor.fetchall()]

            if orphans:
                cursor.execute("""
                    UPDATE tasks SET status = 'failed'
                    WHERE status IN ('running', 'pending') AND external_task_id IS NULL
                """)
                released = 0
                for o in orphans:
                    if o['account_email']:
                        cursor.execute(
                            "UPDATE accounts SET used = 0 "
                            "WHERE api_key_id = %s AND email = %s",
                            (o['api_key_id'], o['account_email'])
                        )
                        released += 1
                print(f"[Cleanup] {len(orphans)} orphan task failed yapıldı, {released} hesap serbest bırakıldı.")
            else:
                print("[Cleanup] Orphan task yok.")

        conn.commit()
        conn.close()


def _execute_query(query, params=None, fetch_one=False, fetch_all=False):
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor) if DB_TYPE == 'postgresql' else conn.cursor()
        try:
            cursor.execute(query, params) if params else cursor.execute(query)
            result = None
            if fetch_one:
                row = cursor.fetchone()
                result = dict(row) if row else None
            elif fetch_all:
                result = [dict(r) for r in cursor.fetchall()]
            else:
                conn.commit()
                result = cursor.lastrowid if DB_TYPE != 'postgresql' and cursor.lastrowid else cursor.rowcount
            return result
        finally:
            conn.close()


# --- API Key Functions ---

def get_api_key_id(key):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    result = _execute_query(f'SELECT id FROM api_keys WHERE key = {ph}', (key,), fetch_one=True)
    return result['id'] if result else None


def create_api_key(key):
    with db_lock:
        conn = get_connection()
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                cursor.execute('INSERT INTO api_keys (key) VALUES (%s) RETURNING id', (key,))
                result = cursor.fetchone()
                conn.commit()
                conn.close()
                return result['id']
            except psycopg2.IntegrityError:
                conn.rollback()
                conn.close()
                return get_api_key_id(key)
        else:
            cursor = conn.cursor()
            try:
                cursor.execute('INSERT INTO api_keys (key) VALUES (?)', (key,))
                conn.commit()
                lid = cursor.lastrowid
                conn.close()
                return lid
            except Exception:
                conn.close()
                return get_api_key_id(key)


def get_all_api_keys():
    return _execute_query('SELECT id, key, created_at FROM api_keys ORDER BY created_at DESC', fetch_all=True)


def delete_api_key(api_key_id):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f'DELETE FROM tasks WHERE api_key_id = {ph}', (api_key_id,))
        cursor.execute(f'DELETE FROM accounts WHERE api_key_id = {ph}', (api_key_id,))
        cursor.execute(f'DELETE FROM api_keys WHERE id = {ph}', (api_key_id,))
        conn.commit()
        conn.close()
        return True


def clear_all_usage_data():
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks')
        cursor.execute('DELETE FROM accounts')
        conn.commit()
        conn.close()
        return True


def reset_all_accounts_usage():
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE accounts SET used = 0')
        conn.commit()
        conn.close()
        return True


def get_or_create_api_key(key):
    api_key_id = get_api_key_id(key)
    if api_key_id is None:
        api_key_id = create_api_key(key)
    return api_key_id


# --- Account Functions ---

def add_account(api_key_id, email, password):
    with db_lock:
        conn = get_connection()
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor()
            try:
                cursor.execute(
                    'INSERT INTO accounts (api_key_id, email, password) VALUES (%s, %s, %s)',
                    (api_key_id, email, password)
                )
                conn.commit()
                conn.close()
                return True
            except psycopg2.IntegrityError:
                conn.rollback()
                conn.close()
                return False
        else:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    'INSERT INTO accounts (api_key_id, email, password) VALUES (?, ?, ?)',
                    (api_key_id, email, password)
                )
                conn.commit()
                conn.close()
                return True
            except Exception:
                conn.close()
                return False


def get_all_accounts(api_key_id):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    return _execute_query(
        f'SELECT email, password, used FROM accounts WHERE api_key_id = {ph}',
        (api_key_id,), fetch_all=True
    )


def get_account_count(api_key_id):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    result = _execute_query(
        f'SELECT COUNT(*) as count FROM accounts WHERE api_key_id = {ph} AND used = 0',
        (api_key_id,), fetch_one=True
    )
    return result['count'] if result else 0


def get_next_account(api_key_id):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor) if DB_TYPE == 'postgresql' else conn.cursor()
        cursor.execute(
            f'SELECT email, password FROM accounts WHERE api_key_id = {ph} AND used = 0 LIMIT 1',
            (api_key_id,)
        )
        row = cursor.fetchone()
        if row:
            account = dict(row)
            cursor.execute(
                f'UPDATE accounts SET used = 1 WHERE api_key_id = {ph} AND email = {ph}',
                (api_key_id, account['email'])
            )
            conn.commit()
        conn.close()
        return dict(row) if row else None


def release_account(api_key_id, email):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    _execute_query(
        f'UPDATE accounts SET used = 0 WHERE api_key_id = {ph} AND email = {ph}',
        (api_key_id, email)
    )
    return True


def delete_account(api_key_id, email):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    result = _execute_query(
        f'DELETE FROM accounts WHERE api_key_id = {ph} AND email = {ph}',
        (api_key_id, email)
    )
    return result > 0


# --- Task Functions ---

def create_task(api_key_id, task_id, mode):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    _execute_query(
        f'INSERT INTO tasks (api_key_id, task_id, mode, status) VALUES ({ph}, {ph}, {ph}, {ph})',
        (api_key_id, task_id, mode, 'pending')
    )


def update_task_account(task_id, account_email):
    """Task'a hangi hesabın atandığını kaydeder. Login başarılı olunca hemen çağrılır."""
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    _execute_query(
        f'UPDATE tasks SET account_email = {ph} WHERE task_id = {ph}',
        (account_email, task_id)
    )


def update_task_status(task_id, status, result_url=None):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    if result_url:
        _execute_query(
            f'UPDATE tasks SET status = {ph}, result_url = {ph} WHERE task_id = {ph}',
            (status, result_url, task_id)
        )
    else:
        _execute_query(
            f'UPDATE tasks SET status = {ph} WHERE task_id = {ph}',
            (status, task_id)
        )


def add_task_log(task_id, message):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor) if DB_TYPE == 'postgresql' else conn.cursor()
        cursor.execute(f'SELECT logs FROM tasks WHERE task_id = {ph}', (task_id,))
        row = cursor.fetchone()
        if row:
            logs = json.loads(dict(row)['logs'] or '[]')
            logs.append({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "message": message})
            cursor.execute(f'UPDATE tasks SET logs = {ph} WHERE task_id = {ph}', (json.dumps(logs), task_id))
            conn.commit()
        conn.close()


def get_task(api_key_id, task_id):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    result = _execute_query(
        f'SELECT task_id, mode, status, result_url, logs, created_at FROM tasks WHERE api_key_id = {ph} AND task_id = {ph}',
        (api_key_id, task_id), fetch_one=True
    )
    if result and result.get('logs'):
        result['logs'] = json.loads(result['logs'])
    return result


def get_all_tasks(api_key_id):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    return _execute_query(
        f'SELECT task_id, mode, status, result_url, created_at FROM tasks WHERE api_key_id = {ph} ORDER BY created_at DESC',
        (api_key_id,), fetch_all=True
    )


def get_running_task_count():
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor) if DB_TYPE == 'postgresql' else conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM tasks WHERE status IN ('running', 'pending')")
        row = cursor.fetchone()
        conn.close()
        return dict(row)['count'] if row else 0


def update_task_external_data(task_id, external_task_id, token):
    ph = '%s' if DB_TYPE == 'postgresql' else '?'
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f'UPDATE tasks SET external_task_id = {ph}, token = {ph} WHERE task_id = {ph}',
            (external_task_id, token, task_id)
        )
        conn.commit()
        conn.close()


def get_incomplete_tasks():
    """Recovery için: external_task_id olan running/pending taskler + api_key_id."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor) if DB_TYPE == 'postgresql' else conn.cursor()
        cursor.execute(
            "SELECT task_id, mode, external_task_id, token, api_key_id FROM tasks "
            "WHERE status IN ('running', 'pending') AND external_task_id IS NOT NULL"
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]
