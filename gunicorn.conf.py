# gunicorn.conf.py (yeni dosya)
def post_fork(server, worker):
    import threading
    import main
    threading.Thread(target=main._run_startup, daemon=True).start()
