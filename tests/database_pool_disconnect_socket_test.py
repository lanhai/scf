#!/usr/bin/env python3
"""Exercise actual PDO against a tiny localhost MySQL protocol fixture, without a database."""
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading

root = Path(__file__).resolve().parents[1]
queries = []
commands = []
errors = []
connections = []
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen()
listener.settimeout(0.2)
stop = threading.Event()


def packet(conn, payload, sequence=0):
    conn.sendall(len(payload).to_bytes(3, 'little') + bytes([sequence]) + payload)


def receive(conn):
    def exact(size):
        data = b''
        while len(data) < size:
            part = conn.recv(size - len(data))
            if not part:
                raise EOFError()
            data += part
        return data
    header = exact(4)
    return exact(int.from_bytes(header[:3], 'little'))


def result(conn, value):
    def field(s):
        return bytes([len(s)]) + s
    column = b''.join(field(s) for s in [b'def', b'', b'', b'', b'value', b''])
    column += b'\x0c' + struct.pack('<HIBHB', 33, 20, 3, 0, 0) + b'\0\0'
    eof = b'\xfe\0\0\x02\0'
    for seq, payload in enumerate([b'\x01', column, eof, field(str(value).encode()), eof], 1):
        packet(conn, payload, seq)


def serve(conn, connection_id):
    try:
        conn.settimeout(5)
        capabilities = 0x00000001 | 0x00000200 | 0x00002000 | 0x00008000 | 0x00080000
        greeting = b'\x0a5.7.44-fixture\0' + struct.pack('<I', connection_id) + b'12345678\0'
        greeting += struct.pack('<H', capabilities & 0xffff) + b'\x21\x02\0'
        greeting += struct.pack('<H', capabilities >> 16) + b'\x15' + b'\0' * 10
        greeting += b'abcdefghijkl\0mysql_native_password\0'
        packet(conn, greeting)
        receive(conn)  # Test-only credentials: no authentication or production data.
        packet(conn, b'\0\0\0\x02\0\0\0', 2)
        while not stop.is_set():
            payload = receive(conn)
            commands.append((connection_id, payload[0]))
            if payload[0] == 1:
                break
            if payload[0] != 3:
                raise RuntimeError('Unexpected MySQL command')
            sql = payload[1:].decode()
            queries.append((connection_id, sql))
            if sql not in ('SELECT 1', 'SELECT 7', 'SELECT 99'):
                raise RuntimeError('Unexpected SQL: ' + sql)
            result(conn, int(sql.split()[1]))
            if sql == 'SELECT 7':
                # Same seam as production: the pool still owns PDO after its peer has closed.
                conn.shutdown(socket.SHUT_RDWR)
                break
    except (EOFError, ConnectionResetError):
        pass
    except Exception as error:
        if not stop.is_set():
            errors.append(str(error))
    finally:
        conn.close()


def accept():
    connection_id = 0
    while not stop.is_set():
        try:
            conn, _ = listener.accept()
        except socket.timeout:
            continue
        connection_id += 1
        worker = threading.Thread(target=serve, args=(conn, connection_id), daemon=True)
        connections.append(worker)
        worker.start()


thread = threading.Thread(target=accept, daemon=True)
thread.start()
php = os.environ.get('SCF_PDO_TEST_PHP') or shutil.which('php')
module = root / 'tools/swoole-sigpipe/build/swoole-src-5.1.8/modules/swoole.so'
command = [php]
if module.is_file():
    command += ['-n', '-d', 'extension=' + str(module)]
idle_mode = '--idle' in sys.argv[1:]
test_file = 'database_pool_idle_reaper_behavior_test.php' if idle_mode else 'database_pool_checkout_behavior_test.php'
command += [str(root / 'tests' / test_file), '--tcp', str(listener.getsockname()[1])]
process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
try:
    out, err = process.communicate(timeout=15)
except subprocess.TimeoutExpired:
    os.killpg(process.pid, signal.SIGKILL)
    process.communicate()
    raise SystemExit('PDO socket regression timed out')
finally:
    stop.set()
    thread.join(timeout=1)
    listener.close()
    for worker in connections:
        worker.join(timeout=1)
print(out + err, end='')
if errors:
    raise SystemExit('Fixture errors: ' + repr(errors))
if process.returncode:
    raise SystemExit(process.returncode)
if idle_mode:
    if queries != [(1, 'SELECT 99')] or commands != [(1, 3), (1, 1)] or len(connections) != 1:
        raise SystemExit('Idle cleanup sent SQL or opened replacement connection: ' + repr((queries, commands)))
elif (2, 'SELECT 1') not in queries or (2, 'SELECT 99') not in queries:
    raise SystemExit('Replacement was not probed before business SQL: ' + repr(queries))
print('PASS wire sequence:', queries)
