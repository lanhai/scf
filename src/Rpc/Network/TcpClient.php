<?php


namespace Scf\Rpc\Network;


use Exception;
use Scf\Rpc\Protocol\Protocol;
use Scf\Rpc\Protocol\Request;
use Scf\Rpc\Protocol\Response;
use Scf\Rpc\Server\ServiceNode;
use Swoole\Coroutine\Client;

class TcpClient {
    private Client $client;

    public function __construct(int $maxPackSize, float $timeout) {
        $this->client = new Client(SWOOLE_TCP);
        $this->client->set([
            'open_length_check' => true,
            'package_length_type' => 'N',
            'package_length_offset' => 0,
            'package_body_offset' => 4,
            'package_max_length' => $maxPackSize,
            'timeout' => $timeout
        ]);
    }

    public function connect(ServiceNode $node, float $timeout = 60): bool {
        return $this->client->connect($node->getIp(), $node->getPort(), $timeout);
    }

    public function sendRequest(Request $request): bool {
        $data = $request->__tostring();
        $data = Protocol::pack($data);
        $len = strlen($data);
        if ($this->client->send($data) !== $len) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * @throws Exception
     */
    public function recv(float $timeout = 60): Response {
        $res = new Response();
        $data = $this->client->recv($timeout);
        if ($data) {
            $data = Protocol::unpack($data);
            $json = json_decode($data, true);
            if (is_array($json)) {
                $res->restore($json);
            } else {
                $res->setStatus(Response::STATUS_ILLEGAL_PACKAGE);
            }
        } else {
            $res->setStatus(Response::STATUS_SERVER_TIMEOUT);
        }
        return $res;
    }

    function __destruct() {
        if ($this->client->isConnected()) {
            $this->client->close();
        }
        unset($this->client);
    }
}