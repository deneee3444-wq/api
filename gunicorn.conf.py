# gunicorn.conf.py (yeni dosya)
def post_fork(server, worker):
    import threading
    import api
    threading.Thread(target=api._run_startup, daemon=True).start()
