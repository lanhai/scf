<?php

namespace Scf\Rpc;

use Scf\Core\Traits\Singleton;
use Scf\Rpc\Service\AbstractService;


class Manager {
    use Singleton;

    private array $serviceRegisterArray = [];
    private int $version = 0;

    /**
     * @return array
     */
    public function getServiceRegisterArray(): array {
        return $this->serviceRegisterArray;
    }


    public function addService(AbstractService $service): Manager {
        $this->serviceRegisterArray[$service->serviceName()] = $service;
        return $this;
    }
}