#!/usr/bin/env python3
"""Real phpredis + Mix Redis + SCF pool against an isolated, stateless RESP fixture."""
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import threading

root = Path(__file__).resolve().parents[1]
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen()
listener.settimeout(0.2)
stop = threading.Event()
commands = []
errors = []
workers = []


def serve(conn):
    try:
        conn.settimeout(3)
        with conn, conn.makefile('rb') as stream:
            while not stop.is_set():
                header = stream.readline()
                if not header:
                    break
                if not header.startswith(b'*'):
                    raise ValueError('Expected RESP array')
                args = []
                for _ in range(int(header[1:])):
                    size = int(stream.readline()[1:])
                    args.append(stream.read(size))
                    if stream.read(2) != b'\r\n':
                        raise ValueError('Invalid RESP terminator')
                command = args[0].upper()
                commands.append(command)
                if command == b'SELECT':
                    conn.sendall(b'+OK\r\n')
                elif command == b'GET':
                    conn.sendall(b'$5\r\nvalue\r\n')
                elif command == b'INVALID_FIXTURE_COMMAND':
                    conn.sendall(b'-ERR fixture command failed\r\n')
                else:
                    raise ValueError('Unexpected Redis command: ' + repr(command))
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as error:
        if not stop.is_set():
            errors.append(str(error))


def accept():
    while not stop.is_set():
        try:
            conn, _ = listener.accept()
        except socket.timeout:
            continue
        worker = threading.Thread(target=serve, args=(conn,), daemon=True)
        workers.append(worker)
        worker.start()


thread = threading.Thread(target=accept, daemon=True)
thread.start()
php = os.environ.get('SCF_PDO_TEST_PHP') or shutil.which('php')
module = root / 'tools/swoole-sigpipe/build/swoole-src-5.1.8/modules/swoole.so'
command = [php]
if module.is_file():
    extension_dir = subprocess.check_output([php, '-r', 'echo ini_get("extension_dir");'], text=True)
    command += ['-n', '-d', 'extension=' + str(module), '-d', 'extension=' + extension_dir + '/redis.so']
port = str(listener.getsockname()[1])
if '--reload' in sys.argv[1:]:
    test = str(root / 'tests/database_pool_idle_worker_exit_test.php')
    runs = [[test, '--redis', port], [test, '--task', '--redis', port]]
else:
    runs = [[str(root / 'tests/redis_shared_pool_behavior_test.php'), port]]
try:
    for options in runs:
        process = subprocess.Popen(command + options, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True)
        try:
            out, err = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise SystemExit('Redis shared pool regression timed out')
        print(out + err, end='', flush=True)
        if process.returncode:
            raise SystemExit(process.returncode)
finally:
    stop.set()
    thread.join(timeout=1)
    listener.close()
    for worker in workers:
        worker.join(timeout=1)
if errors:
    raise SystemExit('RESP fixture errors: ' + repr(errors))
if b'PING' in commands or any(cmd not in (b'SELECT', b'GET', b'INVALID_FIXTURE_COMMAND') for cmd in commands):
    raise SystemExit('Unexpected maintenance command: ' + repr(commands))
print('PASS wire: only connection SELECT and requested business commands; no maintenance PING')
