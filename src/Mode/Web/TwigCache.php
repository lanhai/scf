<?php

namespace Scf\Mode\Web;

class TwigCache {

    private static array $prepared = [];

    public static function prepare(string $cacheDir): void {
        if (isset(self::$prepared[$cacheDir])) {
            return;
        }
        self::safeMkdir($cacheDir);
        foreach (range(0, 15) as $high) {
            foreach (range(0, 15) as $low) {
                self::safeMkdir($cacheDir . '/' . dechex($high) . dechex($low));
            }
        }
        self::$prepared[$cacheDir] = true;
    }

    private static function safeMkdir(string $dir): void {
        if (is_dir($dir)) {
            return;
        }
        $previousHandler = set_error_handler(static function (int $severity, string $message) use ($dir): bool {
            return $severity === E_WARNING && str_contains($message, 'mkdir(): File exists') && is_dir($dir);
        });
        try {
            mkdir($dir, 0777, true);
        } finally {
            restore_error_handler();
        }
        clearstatcache(true, $dir);
    }
}
