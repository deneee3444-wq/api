"""
Database Module for API
Supports both SQLite (local) and PostgreSQL (production).
Set DATABASE_URL environment variable for PostgreSQL.
"""
import os
import json
import threading
from datetime import datetime

# PostgreSQL Configuration
DATABASE_URL = "postgresql://db_mdj5_user:LgPHY1oCy66PW7W2Q0NdBSwH7UDo5vru@dpg-d66v0sjnv86c73dc22fg-a/db_mdj5"

import psycopg2
from psycopg2.extras import RealDictCursor
DB_TYPE = 'postgresql'
print(f"Using PostgreSQL database")

db_lock = threading.Lock()


def get_connection():
    """Returns a database connection."""
    if DB_TYPE == 'postgresql':
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        # Fallback to local SQLite - usually for dev
        import sqlite3
        DB_FILE = "api.db"
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn


def init_db():
    """Initializes the database with required tables."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        
        if DB_TYPE == 'postgresql':
            # PostgreSQL syntax
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Safely add columns to existing PostgreSQL table
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='tasks' AND column_name='external_task_id'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE tasks ADD COLUMN external_task_id TEXT")
            
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='tasks' AND column_name='token'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE tasks ADD COLUMN token TEXT")
                
        else:
            # SQLite syntax
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
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        conn.commit()
        conn.close()


def _execute_query(query, params=None, fetch_one=False, fetch_all=False):
    """Internal helper to execute SQL queries."""
    with db_lock:
        conn = get_connection()
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            result = None
            if fetch_one:
                row = cursor.fetchone()
                if row:
                    result = dict(row)
            elif fetch_all:
                rows = cursor.fetchall()
                result = [dict(row) for row in rows]
            else:
                conn.commit()
                if DB_TYPE != 'postgresql' and cursor.lastrowid:
                    result = cursor.lastrowid
                elif cursor.rowcount is not None:
                    result = cursor.rowcount
            
            return result
        finally:
            conn.close()


# --- API Key Functions ---

def get_api_key_id(key):
    """Returns the ID for a given API key, or None if not found."""
    result = _execute_query(
        'SELECT id FROM api_keys WHERE key = %s' if DB_TYPE == 'postgresql' else 'SELECT id FROM api_keys WHERE key = ?',
        (key,),
        fetch_one=True
    )
    return result['id'] if result else None


def create_api_key(key):
    """Creates a new API key and returns its ID."""
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
                api_key_id = cursor.lastrowid
                conn.close()
                return api_key_id
            except Exception:
                conn.close()
                return get_api_key_id(key)


# --- Admin Functions ---

def get_all_api_keys():
    """Returns all API keys and their info."""
    return _execute_query(
        'SELECT id, key, created_at FROM api_keys ORDER BY created_at DESC',
        fetch_all=True
    )

def delete_api_key(api_key_id):
    """Deletes an API key and its associated data."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        if DB_TYPE == 'postgresql':
            cursor.execute('DELETE FROM tasks WHERE api_key_id = %s', (api_key_id,))
            cursor.execute('DELETE FROM accounts WHERE api_key_id = %s', (api_key_id,))
            cursor.execute('DELETE FROM api_keys WHERE id = %s', (api_key_id,))
        else:
            cursor.execute('DELETE FROM tasks WHERE api_key_id = ?', (api_key_id,))
            cursor.execute('DELETE FROM accounts WHERE api_key_id = ?', (api_key_id,))
            cursor.execute('DELETE FROM api_keys WHERE id = ?', (api_key_id,))
        conn.commit()
        conn.close()
        return True

def clear_all_usage_data():
    """Clears all tasks and accounts from the database."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks')
        cursor.execute('DELETE FROM accounts')
        conn.commit()
        conn.close()
        return True

def reset_all_accounts_usage():
    """Resets 'used' status for all accounts."""
    with db_lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE accounts SET used = 0')
        conn.commit()
        conn.close()
        return True

def get_or_create_api_key(key):
    """Gets existing API key ID or creates new one (Internal use only)."""
    api_key_id = get_api_key_id(key)
    if api_key_id is None:
        api_key_id = create_api_key(key)
    return api_key_id


# --- Account Functions ---

def add_account(api_key_id, email, password):
    """Adds an account for a specific API key."""
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
            except:
                conn.close()
                return False


def get_all_accounts(api_key_id):
    """Returns all accounts for a specific API key."""
    return _execute_query(
        'SELECT email, password, used FROM accounts WHERE api_key_id = %s' if DB_TYPE == 'postgresql' else 'SELECT email, password, used FROM accounts WHERE api_key_id = ?',
        (api_key_id,),
        fetch_all=True
    )


def get_account_count(api_key_id):
    """Returns count of available (unused) accounts for an API key."""
    result = _execute_query(
        'SELECT COUNT(*) as count FROM accounts WHERE api_key_id = %s AND used = 0' if DB_TYPE == 'postgresql' else 'SELECT COUNT(*) as count FROM accounts WHERE api_key_id = ? AND used = 0',
        (api_key_id,),
        fetch_one=True
    )
    return result['count'] if result else 0


def get_next_account(api_key_id):
    """Returns the next available account and marks it as used."""
    with db_lock:
        conn = get_connection()
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                'SELECT email, password FROM accounts WHERE api_key_id = %s AND used = 0 LIMIT 1',
                (api_key_id,)
            )
            account = cursor.fetchone()
            if account:
                cursor.execute(
                    'UPDATE accounts SET used = 1 WHERE api_key_id = %s AND email = %s',
                    (api_key_id, account['email'])
                )
                conn.commit()
            conn.close()
            return dict(account) if account else None
        else:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT email, password FROM accounts WHERE api_key_id = ? AND used = 0 LIMIT 1',
                (api_key_id,)
            )
            row = cursor.fetchone()
            if row:
                account = dict(row)
                cursor.execute(
                    'UPDATE accounts SET used = 1 WHERE api_key_id = ? AND email = ?',
                    (api_key_id, account['email'])
                )
                conn.commit()
                conn.close()
                return account
            conn.close()
            return None


def release_account(api_key_id, email):
    """Sets an account used status back to 0 (unused)."""
    _execute_query(
        'UPDATE accounts SET used = 0 WHERE api_key_id = %s AND email = %s' if DB_TYPE == 'postgresql' else 'UPDATE accounts SET used = 0 WHERE api_key_id = ? AND email = ?',
        (api_key_id, email)
    )
    return True


def delete_account(api_key_id, email):
    """Deletes an account."""
    result = _execute_query(
        'DELETE FROM accounts WHERE api_key_id = %s AND email = %s' if DB_TYPE == 'postgresql' else 'DELETE FROM accounts WHERE api_key_id = ? AND email = ?',
        (api_key_id, email)
    )
    return result > 0


# --- Task Functions ---

def create_task(api_key_id, task_id, mode):
    """Creates a new task in the database."""
    _execute_query(
        'INSERT INTO tasks (api_key_id, task_id, mode, status) VALUES (%s, %s, %s, %s)' if DB_TYPE == 'postgresql' else 'INSERT INTO tasks (api_key_id, task_id, mode, status) VALUES (?, ?, ?, ?)',
        (api_key_id, task_id, mode, 'pending')
    )


def update_task_status(task_id, status, result_url=None):
    """Updates the status and result_url of a task."""
    if result_url:
        _execute_query(
            'UPDATE tasks SET status = %s, result_url = %s WHERE task_id = %s' if DB_TYPE == 'postgresql' else 'UPDATE tasks SET status = ?, result_url = ? WHERE task_id = ?',
            (status, result_url, task_id)
        )
    else:
        _execute_query(
            'UPDATE tasks SET status = %s WHERE task_id = %s' if DB_TYPE == 'postgresql' else 'UPDATE tasks SET status = ? WHERE task_id = ?',
            (status, task_id)
        )


def add_task_log(task_id, message):
    """Adds a log message to the task."""
    with db_lock:
        conn = get_connection()
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT logs FROM tasks WHERE task_id = %s', (task_id,))
            row = cursor.fetchone()
            if row:
                logs = json.loads(row['logs'])
                logs.append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "message": message
                })
                cursor.execute('UPDATE tasks SET logs = %s WHERE task_id = %s', (json.dumps(logs), task_id))
                conn.commit()
        else:
            cursor = conn.cursor()
            cursor.execute('SELECT logs FROM tasks WHERE task_id = ?', (task_id,))
            row = cursor.fetchone()
            if row:
                logs = json.loads(row['logs'])
                logs.append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "message": message
                })
                cursor.execute('UPDATE tasks SET logs = ? WHERE task_id = ?', (json.dumps(logs), task_id))
                conn.commit()
        conn.close()


def get_task(api_key_id, task_id):
    """Returns task detail."""
    result = _execute_query(
        'SELECT task_id, mode, status, result_url, logs, created_at FROM tasks WHERE api_key_id = %s AND task_id = %s' if DB_TYPE == 'postgresql' else 'SELECT task_id, mode, status, result_url, logs, created_at FROM tasks WHERE api_key_id = ? AND task_id = ?',
        (api_key_id, task_id),
        fetch_one=True
    )
    if result and result.get('logs'):
        result['logs'] = json.loads(result['logs'])
    return result


def get_all_tasks(api_key_id):
    """Returns all tasks for an API key."""
    rows = _execute_query(
        'SELECT task_id, mode, status, result_url, created_at FROM tasks WHERE api_key_id = %s ORDER BY created_at DESC' if DB_TYPE == 'postgresql' else 'SELECT task_id, mode, status, result_url, created_at FROM tasks WHERE api_key_id = ? ORDER BY created_at DESC',
        (api_key_id,),
        fetch_all=True
    )
    return rows


def get_running_task_count():
    """Returns the count of currently running/pending tasks (across all API keys)."""
    with db_lock:
        conn = get_connection()
        query = "SELECT COUNT(*) as count FROM tasks WHERE status IN ('running', 'pending')"
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
        else:
            cursor = conn.cursor()
            cursor.execute(query)
        row = cursor.fetchone()
        conn.close()
        return dict(row)['count'] if row else 0


def update_task_external_data(task_id, external_task_id, token):
    """Updates external API task ID and token for recovery."""
    with db_lock:
        conn = get_connection()
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE tasks SET external_task_id = %s, token = %s WHERE task_id = %s',
                (external_task_id, token, task_id)
            )
        else:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE tasks SET external_task_id = ?, token = ?, WHERE task_id = ?',
                (external_task_id, token, task_id)
            )
        conn.commit()
        conn.close()


def get_incomplete_tasks():
    """Returns tasks that need recovery."""
    with db_lock:
        conn = get_connection()
        if DB_TYPE == 'postgresql':
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                "SELECT task_id, mode, external_task_id, token FROM tasks WHERE (status = 'running' OR status = 'pending') AND external_task_id IS NOT NULL"
            )
        else:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT task_id, mode, external_task_id, token FROM tasks WHERE (status = 'running' OR status = 'pending') AND external_task_id IS NOT NULL"
            )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
