def post_fork(server, worker):
    import threading
    from main import _run_startup
    threading.Thread(target=_run_startup, daemon=True).start()
