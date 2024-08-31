<?php

namespace Scf\Util;

use Exception;
use FilesystemIterator;
use Generator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;

class Dir {
    protected static array $fileList = [];

    /**
     * 规范化路径，去掉 /../ 和 /./
     * @param string $path 要规范化的路径
     * @return string 规范化后的路径
     */
    public static function normalizePath(string $path): string {
        if ($repath = realpath($path)) {
            return $repath;
        }
        $parts = array(); // 存储路径的各个部分
        $segments = explode('/', $path); // 将路径按 / 分割成各个部分
        foreach ($segments as $segment) {
            if ($segment == '.' || $segment == '') {
                // 忽略当前目录标识和空部分
                continue;
            }
            if ($segment == '..') {
                // 遇到上一级目录标识时，弹出上一个部分
                array_pop($parts);
            } else {
                // 否则将部分加入数组
                $parts[] = $segment;
            }
        }
        return '/' . implode('/', $parts);
    }

    /**
     * 清空目标目录
     * @param $dir
     * @return bool
     */
    public static function clear($dir): bool {
        if (!file_exists($dir)) {
            return true; // 目录不存在，直接返回成功
        }
        // 遍历目录中的文件和子目录
        $files = array_diff(scandir($dir), ['.', '..']);
        foreach ($files as $file) {
            $path = $dir . '/' . $file;

            // 如果是目录，递归删除目录
            if (is_dir($path)) {
                self::clear($path);
            } else {
                // 如果是文件，直接删除
                unlink($path);
            }
        }
        // 删除目录
        return rmdir($dir);
    }

    /**
     * 复制文件夹
     * @param $source
     * @param $destination
     * @return void
     */
    public static function copy($source, $destination): void {
        if (!is_dir($destination)) {
            mkdir($destination, 0777, true); // 创建目标文件夹，包括子目录
        }
        $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($source, FilesystemIterator::SKIP_DOTS),
            RecursiveIteratorIterator::SELF_FIRST
        );
        foreach ($iterator as $item) {
            $target = $destination . DIRECTORY_SEPARATOR . $iterator->getSubPathName();

            if ($item->isDir()) {
                if (!is_dir($target)) {
                    mkdir($target); // 创建子目录
                }
            } else {
                copy($item, $target); // 复制文件
            }
        }
    }

    /**
     * 扫描文件夹下所有文件
     * @param string $dir 目标目录
     * @param int $deep 递归深度，-1表示无限制
     * @return array 返回文件路径的数组
     */
    public static function scanLongFiles(string $dir, int $deep = -1): array {
        return iterator_to_array(self::scanLongFilesGenerator($dir, $deep));
    }

    /**
     * 使用生成器逐步扫描文件夹下所有文件
     * @param string $dir 目标目录
     * @param int $deep 递归深度，-1表示无限制
     * @return Generator 逐步返回文件路径
     */
    public static function scanLongFilesGenerator(string $dir, int $deep = -1): Generator {
        try {
            $iterator = new RecursiveIteratorIterator(
                new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS),
                RecursiveIteratorIterator::SELF_FIRST
            );

            foreach ($iterator as $fileInfo) {
                // 只获取文件，忽略目录
                if ($fileInfo->isFile()) {
                    yield $fileInfo->getPathname();
                }

                // 如果设置了递归深度限制，且当前深度达到限制
                if ($deep != -1 && $iterator->getDepth() >= $deep) {
                    // 跳过当前目录
                    $iterator->next();
                    while ($iterator->valid() && $iterator->getDepth() >= $deep) {
                        $iterator->next();
                    }
                }
            }
        } catch (Exception $e) {
            error_log("Error scanning directory {$dir}: " . $e->getMessage());
        }
    }

    /**
     * 扫描文件夹下所有文件(不兼容超长文件名)
     * @param string $dir 目录路径
     * @param int $deep 递归深度，-1 表示无限深度
     * @return array 返回所有文件路径的数组
     */
    public static function scan(string $dir, int $deep = -1): array {
        return self::_scan($dir, $deep);
    }

    /**
     * 递归扫描目录
     * @param string $dir 目录路径
     * @param int $deep 递归深度，-1 表示无限深度
     * @param int $level 当前递归深度
     * @return array 返回目录中的文件列表
     */
    private static function _scan(string $dir, int $deep = -1, int $level = 1): array {
        $files = [];
        if (is_dir($dir)) {
            if ($handle = opendir($dir)) {
                while (($file = readdir($handle)) !== false) {
                    if ($file != "." && $file != "..") {
                        $filePath = $dir . DIRECTORY_SEPARATOR . $file;
                        if (is_dir($filePath)) {
                            if ($deep != -1 && $level >= $deep) {
                                continue;
                            }
                            $files = array_merge($files, self::_scan($filePath, $deep, $level + 1));
                        } else {
                            $files[] = $filePath;
                        }
                    }
                }
                closedir($handle);
            } else {
                echo "Could not open directory: $dir\n";
            }
        } else {
            echo "Not a directory: $dir\n";
        }
        return $files;
//        $files = [];
//        if (is_dir($dir)) {
//            $items = scandir($dir);
//            foreach ($items as $item) {
//                // 跳过 "." 和 ".."
//                if ($item === "." || $item === "..") {
//                    continue;
//                }
//                $fullPath = $dir . DIRECTORY_SEPARATOR . $item;
//                if (is_dir($fullPath)) {
//                    // 如果设置了深度限制且达到深度，跳过子目录
//                    if ($deep != -1 && $level >= $deep) {
//                        continue;
//                    }
//                    // 递归扫描子目录
//                    $files = array_merge($files, self::_scan($fullPath, $deep, $level + 1));
//                } else {
//                    // 保存文件路径
//                    $files[] = $fullPath;
//                }
//            }
//        }
//        return $files;
    }
}