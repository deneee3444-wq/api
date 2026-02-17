"""
Database Module for API
PostgreSQL with connection pooling - no threading locks needed.
"""
import os
import json
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://db_mdj5_user:LgPHY1oCy66PW7W2Q0NdBSwH7UDo5vru@dpg-d66v0sjnv86c73dc22fg-a/db_mdj5"
)

_pool = None

def get_pool():
    global _pool
    if _pool is None or _pool.closed:
        _pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=DATABASE_URL,
            connect_timeout=10
        )
    return _pool

def get_connection():
    return get_pool().getconn()

def release_connection(conn):
    try:
        get_pool().putconn(conn)
    except Exception:
        pass

def _execute_query(query, params=None, fetch_one=False, fetch_all=False):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)

        result = None
        if fetch_one:
            row = cursor.fetchone()
            result = dict(row) if row else None
        elif fetch_all:
            rows = cursor.fetchall()
            result = [dict(r) for r in rows]
        else:
            conn.commit()
            result = cursor.rowcount

        return result
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise e
    finally:
        if conn:
            release_connection(conn)


def init_db():
    conn = None
    try:
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

        # Add missing columns safely
        for col, col_type in [('external_task_id', 'TEXT'), ('token', 'TEXT'), ('account_email', 'TEXT')]:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='tasks' AND column_name=%s",
                (col,)
            )
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE tasks ADD COLUMN {col} {col_type}")

        conn.commit()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Database init error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            release_connection(conn)


# --- API Key Functions ---

def get_api_key_id(key):
    result = _execute_query('SELECT id FROM api_keys WHERE key = %s', (key,), fetch_one=True)
    return result['id'] if result else None

def create_api_key(key):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('INSERT INTO api_keys (key) VALUES (%s) RETURNING id', (key,))
        result = cursor.fetchone()
        conn.commit()
        return result['id']
    except psycopg2.IntegrityError:
        if conn:
            conn.rollback()
        return get_api_key_id(key)
    finally:
        if conn:
            release_connection(conn)

def get_or_create_api_key(key):
    api_key_id = get_api_key_id(key)
    if api_key_id is None:
        api_key_id = create_api_key(key)
    return api_key_id


# --- Admin Functions ---

def get_all_api_keys():
    return _execute_query(
        'SELECT id, key, created_at FROM api_keys ORDER BY created_at DESC',
        fetch_all=True
    )

def delete_api_key(api_key_id):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks WHERE api_key_id = %s', (api_key_id,))
        cursor.execute('DELETE FROM accounts WHERE api_key_id = %s', (api_key_id,))
        cursor.execute('DELETE FROM api_keys WHERE id = %s', (api_key_id,))
        conn.commit()
        return True
    finally:
        if conn:
            release_connection(conn)

def clear_all_usage_data():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks')
        cursor.execute('DELETE FROM accounts')
        conn.commit()
        return True
    finally:
        if conn:
            release_connection(conn)

def reset_all_accounts_usage():
    _execute_query('UPDATE accounts SET used = 0')
    return True


# --- Account Functions ---

def add_account(api_key_id, email, password):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO accounts (api_key_id, email, password) VALUES (%s, %s, %s)',
            (api_key_id, email, password)
        )
        conn.commit()
        return True
    except psycopg2.IntegrityError:
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            release_connection(conn)

def get_all_accounts(api_key_id):
    return _execute_query(
        'SELECT email, password, used FROM accounts WHERE api_key_id = %s',
        (api_key_id,),
        fetch_all=True
    )

def get_account_count(api_key_id):
    result = _execute_query(
        'SELECT COUNT(*) as count FROM accounts WHERE api_key_id = %s AND used = 0',
        (api_key_id,),
        fetch_one=True
    )
    return result['count'] if result else 0

def get_next_account(api_key_id):
    """Atomically get and lock the next available account."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            '''
            SELECT email, password FROM accounts
            WHERE api_key_id = %s AND used = 0
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            ''',
            (api_key_id,)
        )
        account = cursor.fetchone()
        if account:
            cursor.execute(
                'UPDATE accounts SET used = 1 WHERE api_key_id = %s AND email = %s',
                (api_key_id, account['email'])
            )
            conn.commit()
            return dict(account)
        conn.rollback()
        return None
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            release_connection(conn)

def release_account(api_key_id, email):
    _execute_query(
        'UPDATE accounts SET used = 0 WHERE api_key_id = %s AND email = %s',
        (api_key_id, email)
    )
    return True

def delete_account(api_key_id, email):
    result = _execute_query(
        'DELETE FROM accounts WHERE api_key_id = %s AND email = %s',
        (api_key_id, email)
    )
    return result > 0


# --- Task Functions ---

def create_task(api_key_id, task_id, mode):
    _execute_query(
        'INSERT INTO tasks (api_key_id, task_id, mode, status) VALUES (%s, %s, %s, %s)',
        (api_key_id, task_id, mode, 'pending')
    )

def update_task_status(task_id, status, result_url=None):
    if result_url:
        _execute_query(
            'UPDATE tasks SET status = %s, result_url = %s WHERE task_id = %s',
            (status, result_url, task_id)
        )
    else:
        _execute_query(
            'UPDATE tasks SET status = %s WHERE task_id = %s',
            (status, task_id)
        )

def add_task_log(task_id, message):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('SELECT logs FROM tasks WHERE task_id = %s FOR UPDATE', (task_id,))
        row = cursor.fetchone()
        if row:
            logs = json.loads(row['logs'])
            logs.append({
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message": message
            })
            cursor.execute(
                'UPDATE tasks SET logs = %s WHERE task_id = %s',
                (json.dumps(logs), task_id)
            )
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_connection(conn)

def get_task(api_key_id, task_id):
    result = _execute_query(
        'SELECT task_id, mode, status, result_url, logs, created_at FROM tasks WHERE api_key_id = %s AND task_id = %s',
        (api_key_id, task_id),
        fetch_one=True
    )
    if result and result.get('logs'):
        result['logs'] = json.loads(result['logs'])
    return result

def get_all_tasks(api_key_id):
    return _execute_query(
        'SELECT task_id, mode, status, result_url, created_at FROM tasks WHERE api_key_id = %s ORDER BY created_at DESC',
        (api_key_id,),
        fetch_all=True
    )

def get_running_task_count():
    result = _execute_query(
        "SELECT COUNT(*) as count FROM tasks WHERE status IN ('running', 'pending')",
        fetch_one=True
    )
    return result['count'] if result else 0

def update_task_account_email(task_id, account_email):
    _execute_query(
        'UPDATE tasks SET account_email = %s WHERE task_id = %s',
        (account_email, task_id)
    )

def update_task_external_data(task_id, external_task_id, token, account_email):
    _execute_query(
        'UPDATE tasks SET external_task_id = %s, token = %s, account_email = %s WHERE task_id = %s',
        (external_task_id, token, account_email, task_id)
    )

def get_incomplete_tasks():
    return _execute_query(
        "SELECT task_id, mode, external_task_id, token, api_key_id, account_email FROM tasks WHERE status IN ('running', 'pending')",
        fetch_all=True
    )
