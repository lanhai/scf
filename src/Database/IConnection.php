<?php

namespace Scf\Database;

use PDOException;

/**
 * Interface ConnectionInterface
 */
interface IConnection {

    public function debug(\Closure $func): IConnection;

    public function raw(string $sql, ...$values): IConnection;

    public function exec(string $sql, ...$values): IConnection;

    public function table(string $table): IConnection;

    public function select(string ...$fields): IConnection;

    public function join(string $table, string $on, ...$values): IConnection;

    public function leftJoin(string $table, string $on, ...$values): IConnection;

    public function rightJoin(string $table, string $on, ...$values): IConnection;

    public function fullJoin(string $table, string $on, ...$values): IConnection;

    public function where(string $expr, ...$values): IConnection;

    public function or(string $expr, ...$values): IConnection;

    public function order(string $field, string $order): IConnection;

    public function group(string ...$fields): IConnection;

    public function having(string $expr, ...$values): IConnection;

    public function offset(int $length): IConnection;

    public function limit(int $length): IConnection;

    public function lockForUpdate(): IConnection;

    public function sharedLock(): IConnection;

    /**
     * 返回多行
     * @return array
     */
    public function get(): array;

    public function count(string|null $field): int|array;

    public function sum(...$fields): array|int|float|bool;

    /**
     * 返回一行
     * @return array|object|false
     */
    public function first(): object|bool|array;

    /**
     * 返回单个值
     * @param string $field
     * @return mixed
     * @throws PDOException
     */
    public function value(string $field): mixed;

    public function updates(array $data): IConnection;

    public function update(string $field, $value): IConnection;

    public function delete(): IConnection;

    /**
     * 自动事务
     * @param \Closure $closure
     * @throws \Throwable
     */
    public function transaction(\Closure $closure);

    public function beginTransaction(): Transaction;

    public function statement(): \PDOStatement;

    public function lastInsertId(): string;

    public function rowCount(): int;

    public function queryLog(): array;

}
