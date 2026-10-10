#!/bin/bash
set -euo pipefail

# Build a local comparison module; never install it or control running services.
task_dir="$(cd "$(dirname "$0")" && pwd)"
task_build="$task_dir/build"
task_source="$task_build/swoole-src-5.1.8"
task_php_config="${SCF_SIGPIPE_PHP_CONFIG:-$(command -v php-config)}"
task_phpize="$(dirname "$task_php_config")/phpize"

if [[ "$(uname -s)" != Darwin ]]; then
    echo 'This reproduction targets Darwin/kqueue.' >&2
    exit 1
fi
mkdir -p "$task_build"
if [[ ! -f "$task_build/v5.1.8.tar.gz" ]]; then
    curl --fail --silent --show-error --location --retry 2 \
        https://codeload.github.com/swoole/swoole-src/tar.gz/refs/tags/v5.1.8 \
        --output "$task_build/v5.1.8.tar.gz"
fi
if [[ ! -d "$task_source" ]]; then
    tar -xzf "$task_build/v5.1.8.tar.gz" -C "$task_build"
    patch -d "$task_source" -p1 < "$task_dir/swoole-5.1.8-kqueue-sigign.patch"
fi
cd "$task_source"
"$task_phpize" > "$task_build/phpize.log" 2>&1
./configure --with-php-config="$task_php_config" --enable-swoole \
    --enable-openssl --with-openssl-dir=/opt/homebrew/opt/openssl@3 \
    --enable-swoole-curl --enable-sockets --enable-mysqlnd \
    > "$task_build/configure.log" 2>&1
make -j6 > "$task_build/make.log" 2>&1
echo "Isolated module: $task_source/modules/swoole.so"
