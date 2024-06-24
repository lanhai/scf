<?php


namespace Scf\Rpc\Protocol;


use Scf\Spl\SplBean;

class Request extends SplBean {
    /** @var string|null */
    protected ?string $service;
    /** @var string|null */
    protected ?string $module;
    /** @var string|null */
    protected ?string $action;
    /** @var mixed */
    protected mixed $arg;
    /** @var string */
    protected string $requestUUID;
    /** @var mixed */
    protected mixed $clientArg;
    /** @var ?string */
    protected ?string $appid = null;
    /** @var ?string */
    protected ?string $clientIP = null;
    /** @var ?string */
    protected ?string $sign = null;

    /**
     * @return string|null
     */
    public function getService(): ?string {
        return $this->service;
    }

    /**
     * @param string|null $service
     */
    public function setService(?string $service): void {
        $this->service = $service;
    }

    /**
     * @return string|null
     */
    public function getModule(): ?string {
        return $this->module;
    }

    /**
     * @param string|null $module
     */
    public function setModule(?string $module): void {
        $this->module = $module;
    }

    /**
     * @return string|null
     */
    public function getAction(): ?string {
        return $this->action;
    }

    /**
     * @param string|null $action
     */
    public function setAction(?string $action): void {
        $this->action = $action;
    }

    public function setClientIp(?string $ip) {
        $this->clientIP = $ip;
    }

    public function getClientIp(): ?string {
        return $this->clientIP;
    }

    public function setAppid(?string $appid) {
        $this->appid = $appid;
    }

    public function getAppid(): ?string {
        return $this->appid;
    }

    public function setSign(?string $sign) {
        $this->sign = $sign;
    }

    public function getSign(): ?string {
        return $this->sign;
    }

    /**
     * @return mixed
     */
    public function getArg(): mixed {
        return $this->arg;
    }

    /**
     * @param mixed $arg
     */
    public function setArg(mixed $arg): void {
        $this->arg = $arg;
    }

    /**
     * @return string
     */
    public function getRequestUUID(): string {
        return $this->requestUUID;
    }

    /**
     * @param string $requestUUID
     */
    public function setRequestUUID(string $requestUUID): void {
        $this->requestUUID = $requestUUID;
    }

    /**
     * @return mixed
     */
    public function getClientArg(): mixed {
        return $this->clientArg;
    }

    /**
     * @param mixed $clientArg
     */
    public function setClientArg(mixed $clientArg): void {
        $this->clientArg = $clientArg;
    }
}