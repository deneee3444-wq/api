# gunicorn.conf.py
import database as db
workers = 1
preload_app = True
timeout = 600
bind = "0.0.0.0:10000"

def post_fork(server, worker):
    import threading
    from api import resume_incomplete_tasks
    
    def startup():
        import time
        max_wait = 300  # DB için max 2 dakika bekle
        start = time.time()
        while time.time() - start < max_wait:
            try:
                db.init_db()
                resume_incomplete_tasks()
                print("[STARTUP] DB initialized successfully.")
                return
            except Exception as e:
                elapsed = int(time.time() - start)
                print(f"[STARTUP] DB not ready yet ({elapsed}s elapsed), retrying in 5s... {e}")
                time.sleep(5)
        print("[STARTUP] WARNING: Could not initialize DB within timeout!")
    
    threading.Thread(target=startup, daemon=True).start()
