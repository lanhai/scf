<?php


namespace Scf\Rpc\Server;


use Scf\Spl\SplBean;

class ServiceNode extends SplBean {
    /** @var string */
    protected string $service;
    protected int $version = 1;
    /** @var string */
    protected string $nodeId;
    /** @var string */
    protected string $ip;
    /** @var int */
    protected int $port;

    /**
     * @return string
     */
    public function getService(): string {
        return $this->service;
    }

    /**
     * @param string $service
     */
    public function setService(string $service): void {
        $this->service = $service;
    }

    /**
     * @return int
     */
    public function getVersion(): int {
        return $this->version;
    }

    /**
     * @param int $version
     */
    public function setVersion(int $version): void {
        $this->version = $version;
    }

    /**
     * @return string
     */
    public function getNodeId(): string {
        return $this->nodeId;
    }

    /**
     * @param string $nodeId
     */
    public function setNodeId(string $nodeId): void {
        $this->nodeId = $nodeId;
    }

    /**
     * @return string
     */
    public function getIp(): string {
        return $this->ip;
    }

    /**
     * @param string $ip
     */
    public function setIp(string $ip): void {
        $this->ip = $ip;
    }

    /**
     * @return int
     */
    public function getPort(): int {
        return $this->port;
    }

    /**
     * @param int $port
     */
    public function setPort(int $port): void {
        $this->port = $port;
    }
}