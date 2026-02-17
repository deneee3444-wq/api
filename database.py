"""
Database Module - PostgreSQL only, no locks.
"""
import json
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://db_mdj5_user:LgPHY1oCy66PW7W2Q0NdBSwH7UDo5vru@dpg-d66v0sjnv86c73dc22fg-a.oregon-postgres.render.com/db_mdj5"

DB_CONNECT_RETRIES = 3
DB_CONNECT_WAIT    = 10


def get_connection():
    last_err = None
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            return psycopg2.connect(DATABASE_URL, connect_timeout=10)
        except Exception as e:
            last_err = e
            print(f"[DB] Baglanti denemesi {attempt}/{DB_CONNECT_RETRIES} basarisiz: {e}")
            if attempt < DB_CONNECT_RETRIES:
                time.sleep(DB_CONNECT_WAIT)
    raise last_err


def _q(query, params=None, fetch_one=False, fetch_all=False):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(query, params) if params else cursor.execute(query)
        if fetch_one:
            row = cursor.fetchone()
            return dict(row) if row else None
        elif fetch_all:
            return [dict(r) for r in cursor.fetchall()]
        else:
            conn.commit()
            return cursor.rowcount
    finally:
        conn.close()


# --- Init ---

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
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
    for col in ['external_task_id', 'token', 'account_email']:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='tasks' AND column_name=%s",
            (col,)
        )
        if not cursor.fetchone():
            cursor.execute(f"ALTER TABLE tasks ADD COLUMN {col} TEXT")
            print(f"[DB] '{col}' kolonu eklendi.")
    conn.commit()
    conn.close()


def cleanup_orphan_tasks():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
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
                    "UPDATE accounts SET used = 0 WHERE api_key_id = %s AND email = %s",
                    (o['api_key_id'], o['account_email'])
                )
                released += 1
        print(f"[Cleanup] {len(orphans)} orphan task failed yapildi, {released} hesap serbest birakildi.")
    else:
        print("[Cleanup] Orphan task yok.")
    conn.commit()
    conn.close()


# --- API Key Functions ---

def get_api_key_id(key):
    result = _q('SELECT id FROM api_keys WHERE key = %s', (key,), fetch_one=True)
    return result['id'] if result else None


def create_api_key(key):
    conn = get_connection()
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


def get_all_api_keys():
    return _q('SELECT id, key, created_at FROM api_keys ORDER BY created_at DESC', fetch_all=True)


def delete_api_key(api_key_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM tasks WHERE api_key_id = %s', (api_key_id,))
    cursor.execute('DELETE FROM accounts WHERE api_key_id = %s', (api_key_id,))
    cursor.execute('DELETE FROM api_keys WHERE id = %s', (api_key_id,))
    conn.commit()
    conn.close()
    return True


def clear_all_usage_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM tasks')
    cursor.execute('DELETE FROM accounts')
    conn.commit()
    conn.close()
    return True


def reset_all_accounts_usage():
    _q('UPDATE accounts SET used = 0')
    return True


def get_or_create_api_key(key):
    api_key_id = get_api_key_id(key)
    if api_key_id is None:
        api_key_id = create_api_key(key)
    return api_key_id


# --- Account Functions ---

def add_account(api_key_id, email, password):
    conn = get_connection()
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


def get_all_accounts(api_key_id):
    return _q('SELECT email, password, used FROM accounts WHERE api_key_id = %s', (api_key_id,), fetch_all=True)


def get_account_count(api_key_id):
    result = _q('SELECT COUNT(*) as count FROM accounts WHERE api_key_id = %s AND used = 0', (api_key_id,), fetch_one=True)
    return result['count'] if result else 0


def get_next_account(api_key_id):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        'SELECT email, password FROM accounts WHERE api_key_id = %s AND used = 0 LIMIT 1',
        (api_key_id,)
    )
    row = cursor.fetchone()
    if row:
        account = dict(row)
        cursor.execute(
            'UPDATE accounts SET used = 1 WHERE api_key_id = %s AND email = %s',
            (api_key_id, account['email'])
        )
        conn.commit()
    conn.close()
    return dict(row) if row else None


def release_account(api_key_id, email):
    _q('UPDATE accounts SET used = 0 WHERE api_key_id = %s AND email = %s', (api_key_id, email))
    return True


def delete_account(api_key_id, email):
    result = _q('DELETE FROM accounts WHERE api_key_id = %s AND email = %s', (api_key_id, email))
    return result > 0


# --- Task Functions ---

def create_task(api_key_id, task_id, mode):
    _q(
        'INSERT INTO tasks (api_key_id, task_id, mode, status) VALUES (%s, %s, %s, %s)',
        (api_key_id, task_id, mode, 'pending')
    )


def update_task_account(task_id, account_email):
    _q('UPDATE tasks SET account_email = %s WHERE task_id = %s', (account_email, task_id))


def update_task_status(task_id, status, result_url=None):
    if result_url:
        _q('UPDATE tasks SET status = %s, result_url = %s WHERE task_id = %s', (status, result_url, task_id))
    else:
        _q('UPDATE tasks SET status = %s WHERE task_id = %s', (status, task_id))


def add_task_log(task_id, message):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute('SELECT logs FROM tasks WHERE task_id = %s', (task_id,))
    row = cursor.fetchone()
    if row:
        logs = json.loads(dict(row)['logs'] or '[]')
        logs.append({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "message": message})
        cursor.execute('UPDATE tasks SET logs = %s WHERE task_id = %s', (json.dumps(logs), task_id))
        conn.commit()
    conn.close()


def get_task(api_key_id, task_id):
    result = _q(
        'SELECT task_id, mode, status, result_url, logs, created_at FROM tasks WHERE api_key_id = %s AND task_id = %s',
        (api_key_id, task_id), fetch_one=True
    )
    if result and result.get('logs'):
        result['logs'] = json.loads(result['logs'])
    return result


def get_all_tasks(api_key_id):
    return _q(
        'SELECT task_id, mode, status, result_url, created_at FROM tasks WHERE api_key_id = %s ORDER BY created_at DESC',
        (api_key_id,), fetch_all=True
    )


def get_running_task_count():
    result = _q("SELECT COUNT(*) as count FROM tasks WHERE status IN ('running', 'pending')", fetch_one=True)
    return result['count'] if result else 0


def update_task_external_data(task_id, external_task_id, token):
    _q(
        'UPDATE tasks SET external_task_id = %s, token = %s WHERE task_id = %s',
        (external_task_id, token, task_id)
    )


def get_incomplete_tasks():
    return _q(
        "SELECT task_id, mode, external_task_id, token, api_key_id FROM tasks "
        "WHERE status IN ('running', 'pending') AND external_task_id IS NOT NULL",
        fetch_all=True
    )
