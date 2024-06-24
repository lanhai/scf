<?php

namespace Scf\Util;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;

class Dir {
    protected static array $fileList = [];

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
     * @param $dir
     * @param int $deep
     * @return array
     */
    public static function scan($dir, int $deep = -1): array {
        self::$fileList = [];
        $files = self::_scan($dir, $deep);
        self::arrange($files);
        return self::$fileList;
    }

    private static function arrange($files): void {
        foreach ($files as $file) {
            if (is_array($file)) {
                self::arrange($file);
            } else {
                self::$fileList[] = $file;
            }
        }
    }

    private static function _scan($dir, $deep = -1, $level = 1) {
        $files = array();
        if ($handle = opendir($dir)) {
            while (($file = readdir($handle)) !== false) {
                if ($file != ".." && $file != ".") {
                    //排除根目录；
                    if (is_dir($dir . "/" . $file)) {
                        if ($deep != -1 && $deep == $level) {
                            continue;
                        }
                        //如果是子文件夹，就进行递归
                        $files[] = self::_scan($dir . "/" . $file, $deep, $level + 1);
                    } else {
                        //不然就将文件的名字存入数组；
                        $extension = explode('.', $file);
                        $extension = array_pop($extension);
                        $files[] = $dir . '/' . $file;
                    }
                }
            }
            closedir($handle);
            return $files;
        }
    }
}