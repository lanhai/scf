<?php

namespace Scf\Cloud\Wx\Pay\V3\Response;

use Scf\Core\Result;
use Scf\Core\Struct;

class Base extends Struct {
    protected string|int $code = 0;
    protected string $message = 'success';
    protected Result $result;
    protected bool $isSuccess = true;

    public static function getResponse(Result $resut, string $scene = ''): static {
        $response = static::factory(scene: $scene);
        $response->setResult($resut);
        if ($resut->hasError()) {
            $response->code = $resut->getErrCode();
            $response->message = $resut->getMessage();
        }
        if (is_array($resut->getData())) {
            $response->install($resut->getData());
        }
        return $response;
    }

    public function asArray($filterNull = true): array {
        return parent::asArray($filterNull);
    }

    public function getCode(): string|int {
        return $this->code;
    }

    public function getMessage(): string {
        return $this->message;
    }

    public function isOK(): bool {
        if ($this->result->hasError()) {
            $this->isSuccess = false;
        } elseif (!$this->validate()) {
            $this->isSuccess = false;
            $this->code = 'RESPONSE_VALIDATION_ERROR';
            $this->message = "响应数据验证失败:" . implode(';', $this->getErrors());
        }
        return $this->isSuccess;
    }

    protected function setResult(Result $result): void {
        $this->result = $result;
    }
}