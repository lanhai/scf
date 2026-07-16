<?php
declare(strict_types=1);

/**
 * Regression coverage for BETWEEN operators nested in logical groups.
 *
 * Usage:
 * php tests/where_builder_grouped_between_regression.php
 */

require dirname(__DIR__) . '/src/Database/Tools/WhereBuilder.php';

use Scf\Database\Tools\WhereBuilder;

$where = WhereBuilder::create(['deleted' => 0])->and([
    '#OR' => [
        'edit_time[>=<]' => [100, 200],
        'first_review_time[>=<]' => [100, 200],
        'second_review_time[>=<]' => [100, 200],
    ],
]);
$built = $where->build();
$expectedSql = '`deleted` = ? AND (`edit_time` BETWEEN ? AND ? OR `first_review_time` BETWEEN ? AND ? OR `second_review_time` BETWEEN ? AND ?)';
$expectedMatch = [0, 100, 200, 100, 200, 100, 200];

if ($built['sql'] !== $expectedSql) {
    fwrite(STDERR, "Grouped BETWEEN SQL mismatch.\nExpected: {$expectedSql}\nActual:   {$built['sql']}\n");
    exit(1);
}
if ($built['match'] !== $expectedMatch) {
    fwrite(STDERR, 'Grouped BETWEEN bindings mismatch: ' . json_encode($built['match']) . PHP_EOL);
    exit(1);
}
if (substr_count($built['sql'], '?') !== count($built['match'])) {
    fwrite(STDERR, "Grouped BETWEEN placeholder count does not match binding count.\n");
    exit(1);
}

fwrite(STDOUT, "WhereBuilder grouped BETWEEN regression checks passed.\n");
