<?php


namespace Scf\Rpc\Network;


use Scf\Rpc\Config\UdpServiceFinder;
use Scf\Rpc\Protocol\UdpPack;
use Scf\Rpc\Utility\Openssl;
use Swoole\Coroutine\Socket;

class UdpClient
{
    private $config;
    private $clientNodeId;

    function __construct(UdpServiceFinder $finder,string $clientNodeId)
    {
        $this->config = $finder;
        $this->clientNodeId = $clientNodeId;
    }

    function send(UdpPack $pack,string $address,int $port)
    {
        $client = new Socket(AF_INET,SOCK_DGRAM);
        $client->setOption(SOL_SOCKET, SO_BROADCAST, 1);
        $client->sendto( $address, $port,$this->pack2string($pack));
    }

    function broadcast(UdpPack $pack)
    {
        foreach ($this->config->getBroadcastAddress() as $address){
            $address = explode(':', $address);
            $ip = array_shift($address);
            $port = array_shift($address);
            if(empty($port)){
                $port = $this->config->getListenPort();
            }
            $this->send($pack,$ip,$port);
        }
    }

    private function pack2string(UdpPack $pack)
    {
        $pack->setPackTime(time());
        if(!empty($this->config->getEncryptKey())){
            $openssl = new Openssl($this->config->getEncryptKey());
            return $openssl->encrypt((string)$pack);
        }else{
            return (string)$pack;
        }
    }
}