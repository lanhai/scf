<?php

namespace Scf\Component;

use Scf\Core\Coroutine\Component;
use Scf\Helper\JsonHelper;
use Scf\Mode\Web\Lifetime;
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
     * @var Client|null
     */
    protected ?Client $client = null;

    /**
     * 发布广播
     * @param $type
     * @param $content
     * @param ?string $topic
     * @return false
     */
    public function publish($type, $content, string $topic = null): bool {
        if (!isset($this->client)) $this->create();
        if (is_null($this->client)) {
            return false;
        }
        //TODO 使用队列方式
        return Coroutine::create(function () use ($type, $content, $topic) {
            $topic = $topic ?: $this->_config['topic'] ?? 'runtime_log';
            $millisecond = Time::millisecond();
            $message = [
                'type' => $type,
                'time' => date('Y-m-d H:i:s.') . substr($millisecond, -3),
                'env' => Lifetime::isDevEnv() ? 'dev' : 'pro',
                'content' => $content
            ];
            try {
                $this->client->publish($topic, JsonHelper::toJson($message), 0, 1);
            } catch (\Exception) {
                unset($this->client);
                return false;
            }
            return true;
        });
    }

    /**
     * 创建客户端
     * @return bool
     */
    protected function create(): bool {
        if (!isset($this->_config['client_id'])) {
            return false;
        }
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