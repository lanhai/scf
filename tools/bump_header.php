#!/usr/bin/env php
<?php
// Auto-bump @version and @updated in the first docblock of a PHP file
// Usage: php tools/bump_header.php /path/to/file.php

if ($argc < 2) {
    fwrite(STDERR, "Usage: php tools/bump_header.php <file>\n");
    exit(2);
}
$file = $argv[1];
if (!is_file($file) || !is_readable($file)) {
    fwrite(STDERR, "File not readable: {$file}\n");
    exit(2);
}
$code = file_get_contents($file);
if ($code === false) {
    fwrite(STDERR, "Failed to read file: {$file}\n");
    exit(2);
}
// Guard: prevent watcher re-trigger loop (3s cooldown per file)
$lockDir = sys_get_temp_dir() . '/bump_header_locks';
if (!is_dir($lockDir)) { @mkdir($lockDir, 0777, true); }
$lockFile = $lockDir . '/' . md5($file) . '.lock';
$now = time();
$last = @is_file($lockFile) ? (int)@file_get_contents($lockFile) : 0;
if ($now - $last < 3) {
    // Within cooldown window; exit quietly to avoid loop
    exit(0);
}
@file_put_contents($lockFile, (string)$now);

// Find the first docblock before the first class/trait/interface keyword
$pattern = '#/\*\*(?:[^*]|\*(?!/))*\*/\s*(?:abstract\s+|final\s+)?(?:class|trait|interface)\b#si';
if (!preg_match($pattern, $code, $m, PREG_OFFSET_CAPTURE)) {
    // No suitable docblock; nothing to do
    exit(0);
}

$docEndPos = strpos($m[0][0], '*/');
$docStartPosGlobal = $m[0][1];
$docEndPosGlobal = $docStartPosGlobal + $docEndPos + 2; // position just after */
$docblock = substr($code, $docStartPosGlobal, $docEndPos + 2);

// Normalize line endings
$doc = str_replace("\r\n", "\n", $docblock);

// Prepare new values
// Updated time in Asia/Singapore (UTC+8)
$dt = new DateTime('now', new DateTimeZone('Asia/Singapore'));
$updated = $dt->format('Y-m-d H:i:s');

// Bump version: if numeric, increment last segment; else append .1
$versionRegex = '/@version\s+([0-9]+(?:\.[0-9]+)*)/i';
if (preg_match($versionRegex, $doc, $vm)) {
    $ver = $vm[1];
    $newVer = $ver;
    if (preg_match('/^\d+(?:\.\d+)*$/', $ver)) {
        $parts = array_map('intval', explode('.', $ver));
        $i = count($parts) - 1;
        $parts[$i]++;
        // carry over if any part reaches 100
        while ($i > 0 && $parts[$i] >= 100) {
            $parts[$i] = 0;
            $i--;
            $parts[$i]++;
        }
        $newVer = implode('.', $parts);
    } else {
        $newVer = $ver . '.1';
    }
    $doc = preg_replace($versionRegex, '@version ' . $newVer, $doc, 1);
} else {
    // Insert @version before closing */
    $doc = preg_replace('/\n\s*\*\//', "\n * @version 1.0\n */", $doc, 1);
}

// Update or insert @updated
$updatedRegex = '/@updated\s+[^\n\r]*/i';
if (preg_match($updatedRegex, $doc)) {
    $doc = preg_replace($updatedRegex, '@updated ' . $updated, $doc, 1);
} else {
    $doc = preg_replace('/\n\s*\*\//', "\n * @updated {$updated}\n */", $doc, 1);
}

// Write back only if content changed to avoid watcher loops
$newCode = substr($code, 0, $docStartPosGlobal) . $doc . substr($code, $docEndPosGlobal);
if ($newCode === $code) {
    // Nothing changed – exit quietly to prevent re-triggering watchers
    exit(0);
}
file_put_contents($file, $newCode);