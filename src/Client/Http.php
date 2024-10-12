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
    protected ?array $certificate = null;

    public function __construct($protocol, $host, $path = '/', $port = 80, $ssl = false, $certificate = null) {
        $this->host = $host;
        $this->port = $port;
        $this->path = $path;
        $this->protocol = $protocol;
        $this->ssl = $ssl;
        $this->certificate = $certificate;
        $this->client = new Client($host, $port, $ssl);
    }

    /**
     * 创建一个HTTP客户端
     * @param $url
     * @param int $port
     * @param array|null $certificate
     * @return static
     */
    public static function create($url, int $port = 0, array $certificate = null): static {
        $cid = Coroutine::getCid();
        $parsedUrl = parse_url($url);
        $protocol = $parsedUrl['scheme'] ?? 'http';
        $host = $parsedUrl['host'];
        $path = $parsedUrl['path'] ?? '/';
        $port = $parsedUrl['port'] ?? ($protocol == 'https' ? 443 : 80);
        if (!empty($parsedUrl['query'])) {
            $path .= '?' . $parsedUrl['query'];
        }
//        preg_match('~^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?~i', $url, $result);
//        $protocol = $result[2] ?: 'http';
//        $host = $result[4];
//        $path = $result[5] ?: '/';
//        if (!empty($result[6])) {
//            $path .= $result[6];
//        }
//        var_dump($protocol);
//        var_dump($host);
//        var_dump($path);
        //提取URL里的端口号
//        if (str_contains($host, ":")) {
//            $hostArr = explode(":", $host);
//            $host = $hostArr[0];
//            $port = $hostArr[1];
//        }
        $class = static::class;
        $ssl = $protocol == 'https';
        if (!isset(self::$_instances[md5($url) . '_' . $cid])) {
            self::$_instances[md5($url) . '_' . $cid] = new $class($protocol, $host, $path, $port ?: ($ssl ? 443 : 80), $ssl, $certificate);
        }
        if ($cid > 0) {
            Coroutine::defer(function () use ($url, $cid) {
                unset(static::$_instances[md5($url) . '_' . $cid]);
            });
        }
        return self::$_instances[md5($url) . '_' . $cid];
    }

    /**
     * @param $timeout
     * @return array
     */
    protected function options($timeout): array {
        $options = [
            'timeout' => $timeout
        ];
        if (!is_null($this->certificate) && isset($this->certificate['cert']) && isset($this->certificate['key'])) {
            $options['ssl_cert_file'] = $this->certificate['cert'];
            $options['ssl_key_file'] = $this->certificate['key'];
        }
        return $options;
    }

    /**
     * @param string $key
     * @param string $path
     * @return void
     */
    public function addFile(string $key, string $path): void {
        $this->client->addFile($path, $key);
    }

    /**
     * 发送POST请求
     * @param mixed $body
     * @param int $timeout
     * @return Result
     */
    public function post(mixed $body = [], int $timeout = 30): Result {
        try {
            $this->headerInit();
            $this->client->setHeaders($this->headers);
            $this->client->set($this->options($timeout));
            $this->client->post($this->path, $body);
            return $this->getResult();
        } catch (\Exception $exception) {
            return Result::error($exception->getMessage());
        }

    }

    /**
     * 参数设置
     * @param $setting
     * @return void
     */
    public function set($setting): void {
        $this->client->set($setting);
    }

    /**
     * 发送JSON POST请求
     * @param mixed $body
     * @param int $timeout
     * @return Result
     */
    public function JPost(mixed $body = [], int $timeout = 30): Result {
        try {
            $body = $body ? JsonHelper::toJson($body) : "{}";
            $this->headerInit();
            $this->headers['Content-Type'] = 'application/json; charset=utf-8';
            $this->headers['Content-Length'] = strlen($body);
            $this->client->setHeaders($this->headers);
            $this->client->set($this->options($timeout));
            $this->client->post($this->path, $body);
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
            $this->headerInit();
            $this->headers['Content-Type'] = 'text/xml; charset=utf-8';
            $this->headers['Content-Length'] = strlen(ArrayHelper::toXml($body));
            $this->client->set($this->options($timeout));
            $this->client->post($this->path, ArrayHelper::toXml($body));
            return $this->getResult();
        } catch (\Exception $exception) {
            return Result::error($exception->getMessage());
        }
    }

    /**
     * 发送GET请求
     * @param int $timeout
     * @return Result
     */
    public function get(int $timeout = 30): Result {
        try {
            $this->headerInit();
            $this->client->setHeaders($this->headers);
            $this->client->set($this->options($timeout));
            $this->client->get($this->path);
            return $this->getResult();
        } catch (\Exception $exception) {
            return Result::error($exception->getMessage());
        }
    }

    /**
     * 下载文件
     * @param $filePath
     * @param int $timeout
     * @return Result
     */
    public function download($filePath, int $timeout = -1): Result {
        try {
            $this->headerInit();
            $this->client->setHeaders($this->headers);
            $this->client->set($this->options($timeout));
            $this->client->download($this->path, $filePath);
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
     * @return string
     */
    public function body(): string {
        return $this->client->body;
    }

    public function statusCode(): string {
        return $this->client->statusCode;
    }

    /**
     * @return mixed
     */
    public function responseHeaders(): mixed {
        return $this->client->headers;
    }

    public function requestHeaders() {
        return $this->client->requestHeaders;
    }

    public function getHeaders(): mixed {
        return $this->client->headers;
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
     * @param $k
     * @param $v
     * @return void
     */
    public function setHeader($k, $v): void {
        $this->headers[$k] = $v;
    }

    /**
     * 升级为websocket
     * @param $path
     * @return bool
     */
    public function upgrade($path): bool {
        return $this->client->upgrade($path);
    }

    public function close(): void {
        $this->client->close();
    }

    public function getBody(): string {
        return $this->client->body;
    }


    /**
     * @return Result
     */
    protected function getResult(): Result {
        if ($this->client->errCode != 0) {
            return Result::error('请求错误:' . socket_strerror($this->client->errCode) . '(' . $this->client->errMsg . ')', 'REQUEST_FAIL');
        }
        if ($this->client->statusCode != 200) {
            return Result::error('状态码[' . $this->client->statusCode . ']', $this->client->statusCode);
        }
        $body = $this->client->body;
        $this->client->close();
        return Result::success(JsonHelper::is($body) ? JsonHelper::recover($body) : $body);
    }

    /**
     * header初始化
     * @return void
     */
    protected function headerInit(): void {
        $this->headers = $this->headers ?: [
            'Host' => $this->host,
            'User-Agent' => 'Chrome/49.0.2587.3',
            'Accept' => 'text/html,application/xhtml+xml,application/xml,application/json',
            'Accept-Encoding' => 'gzip',
            'Content-Type' => $this->contentType
        ];
    }
}