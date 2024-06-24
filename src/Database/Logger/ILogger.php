<?php

namespace Scf\Database\Logger;

/**
 * Interface LoggerInterface
 */
interface ILogger {

    public function trace(float $time, string $sql, array $bindings, int $rowCount, ?\Throwable $exception): void;

}