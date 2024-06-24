<?php


namespace Scf\Rpc\NodeManager;


use Scf\Rpc\Server\ServiceNode;

interface NodeManagerInterface
{
    function getNodes(string $serviceName,?int $version = null):array;
    function getNode(string $serviceName,?int $version = null):?ServiceNode;
    function failDown(ServiceNode $serviceNode):bool;
    function offline(ServiceNode $serviceNode):bool ;
    function alive(ServiceNode $serviceNode):bool;
}