#!/usr/bin/env python3
"""Keep native server reload/shutdown signals inside isolated process groups."""
import os
from pathlib import Path
import shutil
import signal
import subprocess

root = Path(__file__).resolve().parents[1]
php = os.environ.get('SCF_PDO_TEST_PHP') or shutil.which('php')
module = root / 'tools/swoole-sigpipe/build/swoole-src-5.1.8/modules/swoole.so'
command = [php]
if module.is_file():
    command += ['-n', '-d', 'extension=' + str(module)]
command += [str(root / 'tests/database_pool_idle_worker_exit_test.php')]
for options in [[], ['--task']]:
    process = subprocess.Popen(command + options, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, start_new_session=True)
    try:
        out, err = process.communicate(timeout=12)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise SystemExit('Native worker reload regression timed out: ' + repr(options))
    print(out + err, end='', flush=True)
    if process.returncode:
        raise SystemExit(process.returncode)
