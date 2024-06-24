<?php

declare(strict_types=1);

namespace Scf\Server\SocketIO\Storage;

/**
 * Interface MemoryInterface
 *
 * @package Scf\Server\SocketIO\Storage
 */
interface MemoryInterface {

    /**
     * @param string $key
     * @param string $value
     *
     * @return bool
     */
    public function push(string $key, string $value) : bool;

    /**
     * @param string $key
     *
     * @return string
     */
    public function pop(string $key) : string;
}