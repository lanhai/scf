<?php

namespace Scf\Client;

use Scf\Core\Result;
use Scf\Helper\ArrayHelper;
use Scf\Helper\JsonHelper;
use Swoole\Coroutine;
use Swoole\Coroutine\Http\Client;

class Http {
    protected bool $ssl = false;
    protected string $protocol;
    protected string $host;
    protected int $port = 80;
    protected string $path;
    protected Client $client;
    protected static array $_instances = [];
    protected array $headers = [];
    protected string $contentType = 'text/plain; charset=utf-8';//application/x-www-form-urlencoded

    public function __construct($protocol, $host, $path = '/', $port = 80, $ssl = false) {
        $this->host = $host;
        $this->port = $port;
        $this->path = $path;
        $this->protocol = $protocol;
        $this->ssl = $ssl;

    }

    /**
     * 创建一个HTTP客户端
     * @param $url
     * @param int $port
     * @return static
     */
    public static function create($url, int $port = 0): static {
        $cid = Coroutine::getCid();
        preg_match('~^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?~i', $url, $result);
        $protocol = $result[2] ?: 'http';
        $host = $result[4];
        $path = $result[5] ?: '/';
        if (!empty($result[6])) {
            $path .= $result[6];
        }
        $class = get_called_class();
        $ssl = $protocol == 'https';
        if (!isset(self::$_instances[md5($url) . '_' . $cid])) {
            self::$_instances[md5($url) . '_' . $cid] = new $class($protocol, $host, $path, $port ?: ($ssl ? 443 : 80), $ssl);
        }
        return self::$_instances[md5($url) . '_' . $cid];
    }

    /**
     * 发送POST请求
     * @param mixed $body
     * @param int $timeout
     * @return Result
     */
    public function post(mixed $body = [], int $timeout = 30): Result {
        try {
            $client = new Client($this->host, $this->port, $this->ssl);
            $this->headerInit();
            $client->setHeaders($this->headers);
            $client->set(['timeout' => $timeout]);
            $client->post($this->path, $body);
            $this->client = $client;
            return $this->getResult();
        } catch (\Exception $exception) {
            return Result::error($exception->getMessage());
        }

    }

    /**
     * 发送JSON POST请求
     * @param mixed $body
     * @param int $timeout
     * @return Result
     */
    public function JPost(mixed $body = [], int $timeout = 30): Result {
        try {
            $client = new Client($this->host, $this->port, $this->ssl);
            $this->headerInit();
            $this->headers['Content-Type'] = 'application/json; charset=utf-8';
            $this->headers['Content-Length'] = strlen(JsonHelper::toJson($body));
            $client->setHeaders($this->headers);
            $client->set(['timeout' => $timeout]);
            $client->post($this->path, JsonHelper::toJson($body));
            $this->client = $client;
            return $this->getResult();
        } catch (\Exception $exception) {
            return Result::error($exception->getMessage());
        }
    }
    /**
     * 发送XML POST请求
     * @param mixed $body
     * @param int $timeout
     * @return Result
     */
    public function XPost(mixed $body = [], int $timeout = 30): Result {
        try {
            $client = new Client($this->host, $this->port, $this->ssl);
            $this->headerInit();
            $this->headers['Content-Type'] = 'text/xml; charset=utf-8';
            $this->headers['Content-Length'] = strlen(ArrayHelper::toXml($body));
            $client->setHeaders($this->headers);
            $client->set(['timeout' => $timeout]);
            $client->post($this->path, ArrayHelper::toXml($body));
            $this->client = $client;
            return $this->getResult();
        } catch (\Exception $exception) {
            return Result::error($exception->getMessage());
        }
    }

    /**
     * 申明内容类型
     * @param $contentType
     * @return $this
     */
    public function setContentType($contentType): static {
        $this->contentType = $contentType;
        return $this;
    }

    /**
     * 发送GET请求
     * @param int $timeout
     * @return Result
     */
    public function get(int $timeout = 30): Result {
        try {
            $client = new Client($this->host, $this->port, $this->ssl);
            $this->headerInit();
            $client->setHeaders($this->headers);
            $client->set(['timeout' => $timeout]);
            $client->get($this->path);
            $this->client = $client;
            return $this->getResult();
        } catch (\Exception $exception) {
            return Result::error($exception->getMessage());
        }
    }

    /**
     * 设置header
     * @param $headers
     * @return $this
     */
    public function setHeaders($headers): static {
        $this->headers = $headers;
        return $this;
    }

    /**
     * @return Result
     */
    protected function getResult(): Result {
        if ($this->client->errCode != 0) {
            return Result::error('请求错误:' . socket_strerror($this->client->errCode), 'REQUEST_FAIL');
        }
        if ($this->client->statusCode != 200) {
            return Result::error('请求失败:' . $this->client->statusCode, 'REQUEST_ERROR');
        }
        $body = $this->client->body;
        $this->client->close();
        return Result::success(JsonHelper::is($body) ? JsonHelper::recover($body) : $body);
    }

    /**
     * header初始化
     * @return void
     */
    protected function headerInit() {
        $this->headers = $this->headers ?: [
            'Host' => $this->host,
            'User-Agent' => 'Chrome/49.0.2587.3',
            'Accept' => 'text/html,application/xhtml+xml,application/xml,application/json',
            'Accept-Encoding' => 'gzip',
            'Content-Type' => $this->contentType
        ];
    }
}