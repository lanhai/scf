<?php


namespace Scf\Rpc\Client;


use Scf\Rpc\Server\ServiceNode;
use Scf\Util\Random;

class RequestContext {
    /** @var string */
    private string $requestPath;
    /** @var int|null */
    private ?int $serviceVersion;
    /** @var mixed */
    private mixed $arg;
    /** @var callable|null */
    private $onSuccess;
    /** @var callable|null */
    private $onFail;
    /** @var ServiceNode|null */
    private ?ServiceNode $serviceNode;
    /** @var string */
    private string $requestUUID;
    /** @var ?string */
    protected ?string $appid = null;
    /** @var ?string */
    protected ?string $clientIP = null;
    /** @var ?string */
    protected ?string $sign = null;
    private int $retryTimes = 0;

    protected ?string $host = null;

    public function __construct() {
        $this->requestUUID = Random::makeUUIDV4();
    }

    /**
     * @return string
     */
    public function getRequestUUID(): string {
        return $this->requestUUID;
    }

    /**
     * @return string
     */
    public function getRequestPath(): string {
        return $this->requestPath;
    }

    /**
     * @param string $requestPath
     * @return $this
     */
    public function setRequestPath(string $requestPath): RequestContext {
        $this->requestPath = $requestPath;
        return $this;
    }

    /**
     * @return int|null
     */
    public function getServiceVersion(): ?int {
        return $this->serviceVersion;
    }

    /**
     * @param int|null $serviceVersion
     * @return $this
     */
    public function setServiceVersion(?int $serviceVersion): RequestContext {
        $this->serviceVersion = $serviceVersion;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getArg(): mixed {
        return $this->arg;
    }

    /**
     * @param mixed $arg
     * @return $this
     */
    public function setArg(mixed $arg): RequestContext {
        $this->arg = $arg;
        return $this;
    }

    public function setRequestIp($ip): static {
        $this->clientIP = $ip;
        return $this;
    }

    public function getClientIp(): ?string {
        return $this->clientIP;
    }

    public function setRequestAppid($appid): static {
        $this->appid = $appid;
        return $this;
    }

    public function getClientAppid(): ?string {
        return $this->appid;
    }

    public function setSign($sign): static {
        $this->sign = $sign;
        return $this;
    }

    public function getSign(): ?string {
        return $this->sign;
    }

    /**
     * @return callable|null
     */
    public function getOnSuccess(): ?callable {
        return $this->onSuccess;
    }

    /**
     * @param callable|null $onSuccess
     * @return $this
     */
    public function setOnSuccess(?callable $onSuccess): RequestContext {
        $this->onSuccess = $onSuccess;
        return $this;
    }

    /**
     * @return callable|null
     */
    public function getOnFail(): ?callable {
        return $this->onFail;
    }

    /**
     * @param callable|null $onFail
     * @return $this
     */
    public function setOnFail(?callable $onFail): RequestContext {
        $this->onFail = $onFail;
        return $this;
    }

    public function enableRetry(int $times): RequestContext {
        if ($times >= 0) {
            $this->retryTimes = $times;
        }
        return $this;
    }

    public function getRetryTimes(): int {
        return $this->retryTimes;
    }

    /**
     * @return ServiceNode|null
     */
    public function getServiceNode(): ?ServiceNode {
        return $this->serviceNode;
    }

    /**
     * @param ServiceNode|null $serviceNode
     * @return $this
     */
    public function setServiceNode(?ServiceNode $serviceNode): RequestContext {
        $this->serviceNode = $serviceNode;
        return $this;
    }
}