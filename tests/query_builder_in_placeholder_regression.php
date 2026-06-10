<?php
declare(strict_types=1);

/**
 * QueryBuilder IN placeholder regression checks.
 *
 * These checks do not connect to a database. They exercise the protected SQL
 * builder directly so the placeholder/value order can be verified before PDO
 * receives the query.
 *
 * Usage:
 * php scf/tests/query_builder_in_placeholder_regression.php
 */

require dirname(__DIR__) . '/src/Database/IConnection.php';
require dirname(__DIR__) . '/src/Database/Tools/Expr.php';
require dirname(__DIR__) . '/src/Database/Tools/QueryBuilder.php';

use Scf\Database\IConnection;
use Scf\Database\Tools\QueryBuilder;

/**
 * Minimal IConnection implementation that exposes QueryBuilder::build().
 *
 * The framework QueryBuilder trait is normally mixed into real connection
 * classes. This probe keeps the regression test isolated from database
 * drivers, pools, and application bootstrapping.
 */
class QueryBuilderInPlaceholderProbe implements IConnection {
    use QueryBuilder;

    protected array $config = ['prefix' => ''];

    /**
     * Build the current SELECT SQL without executing it.
     *
     * @return array{0: string, 1: array<int, mixed>}
     */
    public function exposeBuild(): array {
        return $this->build('SELECT');
    }

    public function debug(Closure $func): IConnection { return $this; }
    public function raw(string $sql, ...$values): IConnection { return $this; }
    public function exec(string $sql, ...$values): IConnection { return $this; }
    public function get(): array { return []; }
    public function count(string|null $field): int|array { return 0; }
    public function sum(...$fields): array|int|float|bool { return 0; }
    public function first(): object|bool|array { return false; }
    public function value(string $field): mixed { return null; }
    public function updates(array $data): IConnection { return $this; }
    public function update(string $field, $value): IConnection { return $this; }
    public function delete(): IConnection { return $this; }
    public function transaction(Closure $closure) { return null; }
    public function beginTransaction(): Scf\Database\Transaction { throw new RuntimeException('not used'); }
    public function statement(): PDOStatement { throw new RuntimeException('not used'); }
    public function lastInsertId(): string { return ''; }
    public function rowCount(): int { return 0; }
    public function queryLog(): array { return []; }
}

assertQueryBuilderInPlaceholders(
    '`review_admin` IN (?) AND `first_review_status` = ? AND `second_review_admin` IN (?)',
    [[1262], 0, range(74, 80)],
    'SELECT * FROM t WHERE `review_admin` IN (?) AND `first_review_status` = ? AND `second_review_admin` IN (?,?,?,?,?,?,?)',
    [1262, 0, 74, 75, 76, 77, 78, 79, 80],
    'single-value IN before multi-value IN'
);

assertQueryBuilderInPlaceholders(
    'a IN (?) AND b = ? AND c IN (?)',
    [[1, 2, 3], 9, [4, 5]],
    'SELECT * FROM t WHERE a IN (?,?,?) AND b = ? AND c IN (?,?)',
    [1, 2, 3, 9, 4, 5],
    'multi-value IN before multi-value IN'
);

assertQueryBuilderInPlaceholders(
    'a IN (?) AND b = ? AND c IN (?)',
    [[], 9, [4]],
    'SELECT * FROM t WHERE a IN (NULL) AND b = ? AND c IN (?)',
    [9, 4],
    'empty IN before single-value IN'
);

fwrite(STDOUT, "QueryBuilder IN placeholder regression checks passed.\n");

/**
 * Assert that QueryBuilder expands IN placeholders without shifting values.
 *
 * @param string $whereSql Raw where expression.
 * @param array<int, mixed> $whereValues Values passed to QueryBuilder::where().
 * @param string $expectedSql Expected SQL after QueryBuilder expansion.
 * @param array<int, mixed> $expectedValues Expected bound value order.
 * @param string $caseName Human-readable case name.
 * @return void
 */
function assertQueryBuilderInPlaceholders(
    string $whereSql,
    array $whereValues,
    string $expectedSql,
    array $expectedValues,
    string $caseName
): void {
    [$sql, $values] = (new QueryBuilderInPlaceholderProbe())
        ->table('t')
        ->where($whereSql, ...$whereValues)
        ->exposeBuild();

    if ($sql !== $expectedSql) {
        failCase($caseName, "SQL mismatch\nExpected: {$expectedSql}\nActual:   {$sql}");
    }
    if ($values !== $expectedValues) {
        failCase($caseName, 'Values mismatch: ' . json_encode($values, JSON_UNESCAPED_SLASHES));
    }
    if (substr_count($sql, '?') !== count($values)) {
        failCase($caseName, 'Placeholder count does not match bound value count');
    }
}

/**
 * Print a focused failure message and stop the script.
 *
 * @param string $caseName Human-readable case name.
 * @param string $message Failure detail.
 * @return never
 */
function failCase(string $caseName, string $message): never {
    fwrite(STDERR, "[{$caseName}] {$message}\n");
    exit(1);
}
