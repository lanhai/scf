<?php

namespace Scf\Cloud\Tencent;

class ImApiResult {
    protected array $result;

    public function __construct($result) {
        $this->result = $result;
    }

    /**
     * @param $key
     * @return mixed|null
     */
    public function getResult($key): mixed {
        return $this->result[$key] ?? null;
    }

    public function isOk(): bool {
        if (!isset($this->result['ActionStatus'])) return false;
        return $this->result['ActionStatus'] == 'OK';
    }

    public function getErrCode() {
        return $this->result['ErrorCode'] ?? 'UNKNOW';
    }

    public function getErrInfo() {
        return $this->result['ErrorInfo'] ?? 'UNKNOW';
    }
}