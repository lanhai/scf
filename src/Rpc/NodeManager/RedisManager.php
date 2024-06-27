<?php

namespace Scf\Rpc\NodeManager;

use Scf\Core\Console;
use Scf\Database\Exception\NullMasterDb;
use Scf\Database\Exception\NullPool;
use Scf\Cache\Redis;
use Scf\Rpc\Server\ServiceNode;


class RedisManager implements NodeManagerInterface {
    protected string $redisKey = '_RPC_SERVER_';

    protected int $ttl = 30;

    protected Redis|NullPool|NullMasterDb $pool;

    protected array $config;
    protected string $serverName = 'main';

    public function __construct($serverName = 'main', string $hashKey = '_RPC_SERVER_', int $ttl = 30) {
        $this->serverName = $serverName;
        $this->redisKey = $hashKey;
        $this->ttl = $ttl;
        $pool = $this->createRedisPool();
        if ($pool instanceof NullPool) {
            Console::error("【RPC Manager】RPC Manager created fail:Redis 连接失败");
        }
    }

    protected function createRedisPool(): Redis|NullPool {
        //$this->pool = Redis::instance()->create($this->config);
        $this->pool = Redis::pool($this->serverName, '_rpc_service_');
        return $this->pool;
    }

    public function clear($serviceName): void {
        /** @var Redis $redis */
        $redis = $this->pool;
        try {
            $redis->hGetAll("{$this->redisKey}_{$serviceName}");
        } catch (\Throwable) {
            $this->createRedisPool();
        }

    }

    public function getNodes(string $serviceName, ?int $version = null): array {
        $fails = [];
        $hits = [];
        $time = time();

        /** @var Redis $redis */
        $redis = $this->pool;

        try {
            $nodes = $redis->hGetAll("{$this->redisKey}_{$serviceName}");
            $nodes = $nodes ?: [];
            foreach ($nodes as $nodeId => $value) {
                $node = json_decode($value, true);
                if ($time - $node['lastHeartbeat'] > $this->ttl) {
                    $fails[] = $nodeId;
                    continue;
                }
                if ($node['service'] === $serviceName) {
                    $serviceNode = new ServiceNode($node);
                    $serviceNode->setNodeId(strval($nodeId));
                    if ($version !== null && $version === $node['version']) {
                        $hits[$nodeId] = $serviceNode;
                    } else {
                        $hits[] = $serviceNode;
                    }
                }
            }
            if (!empty($fails)) {
                foreach ($fails as $failKey) {
                    $this->deleteServiceNode($serviceName, $failKey);
                }
            }
            return $hits;
        } catch (\Throwable $throwable) {
            // 如果该 redis 断线则尝试重新连接
            $this->createRedisPool();
        } finally {
            //$redisPool->recycleObj($redis);
        }

        return [];
    }

    public function getNode(string $serviceName, ?int $version = null): ?ServiceNode {
        $list = $this->getNodes($serviceName, $version);
        if (empty($list)) {
            return null;
        }
        $allWeight = 0;
        /** @var Redis $redis */
        $redis = $this->pool;
        $time = time();

        try {
            foreach ($list as $node) {
                /** @var ServiceNode $nodee */
                $key = $node->getNodeId();
                $nodeConfig = $redis->hGet("{$this->redisKey}_{$serviceName}", $key);
                //$nodeConfig = json_decode($nodeConfig, true);
                $lastFailTime = $nodeConfig['lastFailTime'];
                if ($time - $lastFailTime >= 10) {
                    $weight = 10;
                } else {
                    $weight = abs(10 - ($time - $lastFailTime));
                }
                $allWeight += $weight;
                $node->__weight = $weight;
            }
            mt_srand(intval(microtime(true)));
            $allWeight = rand(0, $allWeight - 1);
            foreach ($list as $node) {
                $allWeight = $allWeight - $node->__weight;
                if ($allWeight <= 0) {
                    return $node;
                }
            }
        } catch (\Throwable $throwable) {
            // 如果该 redis 断线则尝试重新连接
            $this->createRedisPool();
        } finally {
            //$redisPool->recycleObj($redis);
        }

        return null;
    }

    public function failDown(ServiceNode $serviceNode): bool {
        /** @var Redis $redis */
        $redis = $this->pool;
        try {
            $serviceName = $serviceNode->getService();
            $nodeId = $serviceNode->getNodeId();
            $hashKey = "{$this->redisKey}_{$serviceName}";
            $nodeConfig = $redis->hGet($hashKey, $nodeId);
            //$nodeConfig = json_decode($nodeConfig, true);
            $nodeConfig['lastFailTime'] = time();
            $redis->hSet($hashKey, $nodeId, json_encode($nodeConfig));
            return true;
        } catch (\Throwable $throwable) {
            // 如果该 redis 断线则尝试重新连接
            $this->createRedisPool();
        } finally {
            //$redisPool->recycleObj($redis);
        }

        return false;
    }

    public function offline(ServiceNode $serviceNode): bool {
        /** @var Redis $redis */
        $redis = $this->pool;
        try {
            $serviceName = $serviceNode->getService();
            $nodeId = $serviceNode->getNodeId();
            $hashKey = "{$this->redisKey}_{$serviceName}";
            $redis->hDel($hashKey, $nodeId);
            return true;
        } catch (\Throwable $throwable) {
            // 如果该 redis 断线则尝试重新连接
            $this->createRedisPool();
        } finally {
            //$redisPool->recycleObj($redis);
        }

        return false;
    }

    public function alive(ServiceNode $serviceNode): bool {
        $info = [
            'service' => $serviceNode->getService(),
            'ip' => $serviceNode->getIp(),
            'port' => $serviceNode->getPort(),
            'version' => $serviceNode->getVersion(),
            'lastHeartbeat' => time(),
            'lastFailTime' => 0
        ];
        /** @var Redis $redis */
        $redis = $this->pool;
        try {
            $serviceName = $serviceNode->getService();
            $nodeId = $serviceNode->getNodeId();
            $hashKey = "{$this->redisKey}_{$serviceName}";
            $redis->hSet($hashKey, $nodeId, json_encode($info));
            return true;
        } catch (\Throwable $throwable) {
            // 如果该 redis 断线则尝试重新连接
            $this->createRedisPool();
        } finally {
            //$redisPool->recycleObj($redis);
        }

        return false;
    }

    private function deleteServiceNode($serviceName, $failKey): void {
        /** @var Redis $redis */
        $redis = $this->pool;
        try {
            $redis->hDel("{$this->redisKey}_{$serviceName}", $failKey);
            return;
        } catch (\Throwable $throwable) {
            // 如果该 redis 断线则尝试重新连接
            $this->createRedisPool();
        } finally {
            //$redisPool->recycleObj($redis);
        }

    }
}