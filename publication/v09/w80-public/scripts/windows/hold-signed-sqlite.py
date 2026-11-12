import pathlib, sqlite3, sys, time
database, ready, stop = map(pathlib.Path, sys.argv[1:])
with sqlite3.connect(database, timeout=1) as connection:
    connection.execute('BEGIN EXCLUSIVE')
    ready.write_text('exclusive-write-lock-held\n')
    deadline = time.monotonic() + 60
    while not stop.exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    connection.rollback()
