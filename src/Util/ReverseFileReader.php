<?php

namespace Scf\Util;

/**
 * 从文件末尾按行倒序分页，不依赖 tac/sed 等外部命令。
 */
final class ReverseFileReader {
    /**
     * 返回与 tac file 加 sed 行范围相同顺序的原始行。
     *
     * @return array<int, string>
     */
    public static function page(string $file, int $offset, int $limit, int $chunkSize = 65_536): array {
        $offset = max(0, $offset);
        $limit = max(0, $limit);
        if ($limit === 0 || !is_file($file)) {
            return [];
        }
        $handle = @fopen($file, 'rb');
        if (!is_resource($handle)) {
            return [];
        }

        try {
            if (@fseek($handle, 0, SEEK_END) !== 0) {
                return [];
            }
            $fileSize = @ftell($handle);
            if (!is_int($fileSize) || $fileSize <= 0) {
                return [];
            }

            $chunkSize = max(4_096, min(1_048_576, $chunkSize));
            $position = $fileSize;
            $buffer = '';
            $seen = 0;
            $target = $offset + $limit;
            $lines = [];
            $skipTerminalEmpty = true;

            while ($position > 0 && $seen < $target) {
                $readSize = min($chunkSize, $position);
                $position -= $readSize;
                if (@fseek($handle, $position, SEEK_SET) !== 0) {
                    break;
                }
                $chunk = @fread($handle, $readSize);
                if (!is_string($chunk) || $chunk === '') {
                    break;
                }
                $buffer = $chunk . $buffer;

                while ($seen < $target && ($newlineAt = strrpos($buffer, "\n")) !== false) {
                    $line = substr($buffer, $newlineAt + 1);
                    $buffer = substr($buffer, 0, $newlineAt);
                    if ($skipTerminalEmpty && $line === '') {
                        $skipTerminalEmpty = false;
                        continue;
                    }
                    $skipTerminalEmpty = false;
                    if ($seen >= $offset) {
                        $lines[] = rtrim($line, "\r");
                    }
                    $seen++;
                }
            }

            if ($position === 0 && $seen < $target && ($buffer !== '' || $fileSize > 0)) {
                if ($seen >= $offset) {
                    $lines[] = rtrim($buffer, "\r");
                }
            }
            return array_slice($lines, 0, $limit);
        } finally {
            @fclose($handle);
        }
    }
}
