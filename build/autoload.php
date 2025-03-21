<?php
spl_autoload_register(function ($class) {
    // 将命名空间 Scf 映射到 PHAR 文件中的 src 目录
    if (str_starts_with($class, 'Scf\\')) {
        $classPath = str_replace('Scf\\', '', $class);
        $filePath = 'phar://' . __DIR__ . '/scf.phar/' . str_replace('\\', '/', $classPath) . '.php';
        if (file_exists($filePath)) {
            require $filePath;
        }
    }
});
