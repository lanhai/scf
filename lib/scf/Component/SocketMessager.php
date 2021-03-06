<?php

namespace Scf\Component;

use Scf\Core\Component;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Thread;
use Scf\Util\Sn;
use Scf\Util\Time;
use Simps\MQTT\Client;
use Simps\MQTT\Config\ClientConfig;
use Swoole\Coroutine;

class SocketMessager extends Component {
    const SWOOLE_MQTT_CONFIG = [
        'open_mqtt_protocol' => true,
        'package_max_length' => 2 * 1024 * 1024,
        'connect_timeout' => 5.0,
        'write_timeout' => 5.0,
        'read_timeout' => 5.0,
    ];
    /**
     * @var Client
     */
    protected Client $client;

    /**
     * 发布广播
     * @param $type
     * @param $content
     * @return false
     */
    public function publish($type, $content): bool {
        if (!isset($this->client)) $this->create();
        return Coroutine::create(function () use ($type, $content) {
            $millisecond = Time::millisecond();
            $message = [
                'type' => $type,
                'time' => date('Y-m-d H:i:s.') . substr($millisecond, -3),
                'env' => Thread::isDevEnv() ? 'dev' : 'pro',
                'content' => $content
            ];
            try {
                $this->client->publish($this->_config['topic'] ?? 'runtime_log', JsonHelper::toJson($message), 0, 1);
            } catch (\Exception) {
                unset($this->client);
                return false;
            }
            return true;
        });
    }

    protected function create(): bool {
        $clientId = $this->_config['client_id'] . '_' . Sn::create_guid();
        $config = new ClientConfig();
        $config->setUserName($this->_config['username'])
            ->setPassword($this->_config['password'])
            ->setClientId($clientId)
            ->setKeepAlive(0)
            ->setDelay(3000) // 3s
            ->setMaxAttempts(-1)
            ->setSwooleConfig(self::SWOOLE_MQTT_CONFIG);
        $client = new Client($this->_config['broker'], $this->_config['port'], $config);
        $this->client = $client;
        $this->client->connect();
        return true;
    }


}