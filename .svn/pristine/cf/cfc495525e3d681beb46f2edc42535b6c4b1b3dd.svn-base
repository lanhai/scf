<?php

namespace Scf\Util;

class Dir {
    protected static array $fileList = [];


    /**
     * 扫描文件夹下所有文件
     * @param $dir
     * @param int $deep
     * @return array
     */
    public static function getDirFiles($dir, int $deep = -1): array {
        self::$fileList = [];
        $files = self::scanDirFiles($dir, $deep);
        self::formatDirFiles($files);
        return self::$fileList;
    }

    private static function formatDirFiles($files) {
        foreach ($files as $file) {
            if (is_array($file)) {
                self::formatDirFiles($file);
            } else {
                self::$fileList[] = $file;
            }
        }
    }

    private static function scanDirFiles($dir, $deep = -1, $level = 1) {
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
                        $files[] = self::scanDirFiles($dir . "/" . $file, $deep, $level + 1);
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