#!/usr/bin/env python3
"""Validate the native backport in separate process groups without installing it."""

import datetime
import hashlib
import os
from pathlib import Path
import shutil
import signal
import subprocess


task_dir = Path(__file__).resolve().parent
task_root = task_dir.parents[1]
task_module = task_dir / "build/swoole-src-5.1.8/modules/swoole.so"
task_php = os.environ.get("SCF_SIGPIPE_PHP") or shutil.which("php")
if not task_module.is_file() or not task_php:
    raise SystemExit("Build the isolated module and select a matching PHP binary first.")

task_records = [
    "Verified at " + datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "Original Swoole 5.1.8 reproduced event worker SIGBUS, PC=1, x0=13.",
    "Backport: skip SIG_IGN in the kqueue signal dispatcher.",
]
for task_test in [
    "event_worker_broken_pipe_behavior_test.php",
    "task_worker_sigpipe_guard_behavior_test.php",
]:
    task_cmd = [task_php, "-n", "-d", "extension=" + str(task_module), str(task_root / "tests" / task_test)]
    # Native server shutdown may signal its process group. Keep the harness outside it.
    task_process = subprocess.Popen(task_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, start_new_session=True)
    try:
        task_out, task_err = task_process.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(task_process.pid, signal.SIGKILL)
        task_process.communicate()
        raise SystemExit(task_test + " timed out")
    task_result = task_test + " exit=" + str(task_process.returncode) + "\n" + task_out + task_err
    print(task_result, flush=True)
    task_records.append(task_result)
    if task_process.returncode:
        raise SystemExit(1)

for task_file in [task_dir / "build/v5.1.8.tar.gz", task_module]:
    task_records.append(str(task_file) + " sha256=" + hashlib.sha256(task_file.read_bytes()).hexdigest())
(task_dir / "validation.txt").write_text("\n\n".join(task_records) + "\n")
