<?php

namespace Scf\Cloud\Tencent;

use ArrayObject;
use Hedeqiang\TenIM\IM;

/**
 * Supplies PHP 8.2-compatible configuration to the Tencent IM SDK while
 * retaining its signing, transport, and response handling.
 */
final class ImClient extends IM {
    /** @param array $config Tencent IM credentials and client settings. */
    public function __construct(array $config) {
        // SDK 1.x only reads flat keys via get(); its own Config has legacy ArrayAccess signatures.
        $this->config = new class($config) extends ArrayObject {
            public function get(string $key): mixed {
                return $this[$key] ?? null;
            }
        };
    }
}
