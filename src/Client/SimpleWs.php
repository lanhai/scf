<?php

namespace Scf\Client;

use Scf\Core\Config;
use Scf\Util\Arr;
use Scf\Util\Sn;

class SimpleWs {
    protected null|string $server;
    protected null|string $appid;
    protected null|string $appkey;
    protected array $config;
    protected Http $client;

    /**
     * 构造器
     */
    public function __construct() {
        $class = static::class;
        $config = Config::get('clients')[$class] ?? [];
        $this->config = $config;
        $this->server = $config['server'] ?? null;
        $this->appid = $config['appid'] ?? null;
        $this->appkey = $config['appkey'] ?? null;


    }

    /**
     * @param $path
     * @return Http
     */
    public static function create($path): Http {
        $class = new static();
        return $class->init($path);
    }

    public function init($path): Http {
        $gateway = $this->server . $path;
        $rand = Sn::create_uuid();
        $timestamp = time();
        $header = [
            'appid' => $this->appid,
            'timestamp' => $timestamp,
            'rand' => $rand,
        ];
        if (!empty($this->config['header'])) {
            $header = Arr::merge($header, $this->config['header']);
        }
        $header['sign'] = md5($this->appid . md5($this->appkey) . $timestamp . $rand);
        $this->client = Http::create($gateway)->setHeaders($header);
        return $this->client;
    }
}