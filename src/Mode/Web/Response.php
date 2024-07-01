<?php

namespace Scf\Mode\Web;


use JetBrains\PhpStorm\NoReturn;
use Scf\Core\Result;
use Scf\Core\Traits\ProcessLifeSingleton;
use Scf\Helper\StringHelper;
use Scf\Server\Env;
use Scf\Server\Worker\ProcessLife;
use Scf\Util\Time;
use Twig\Environment;
use Twig\Error\LoaderError;
use Twig\Error\RuntimeError;
use Twig\Error\SyntaxError;
use Twig\Loader\FilesystemLoader;

class Response {
    use ProcessLifeSingleton;

    /**
     * @var \Swoole\Http\Response
     */
    protected \Swoole\Http\Response $response;
    protected bool $isEnd = false;


    /**
     * @return bool
     */
    public function isEnd(): bool {
        if (!isset($this->response)) {
            return true;
        }
        return $this->isEnd;
    }

    /**
     * 输出成功结果
     * @param mixed $data
     * @return void
     */
    public static function success(mixed $data = ''): void {
        $output = [
            'errCode' => 0,
            'message' => 'SUCCESS',
            'data' => $data
        ];
        self::instance()->json($output);
        //self::instance()->stop(200);
    }

    /**
     * 向客户端响应输出错误
     * @param $error
     * @param string $code
     * @param mixed $data
     * @param int $status
     * @return void
     */
    public static function error($error, string $code = 'SERVICE_ERROR', mixed $data = '', int $status = 503): void {
        if ($error instanceof Result) {
            $output = [
                'errCode' => $error->getErrCode(),
                'message' => $error->getMessage(),
                'data' => $error->getData()
            ];
        } else {
            $output = [
                'errCode' => $code,
                'message' => $error,
                'data' => $data
            ];
        }
        self::instance()->status($status);
        if (Request::isAjax()) {
            self::instance()->json($output);
        } else {
            $loader = new FilesystemLoader(__DIR__ . '/Template');
            $twig = new Environment($loader, [
                'cache' => APP_TMP_PATH . 'template',
                'auto_reload' => true,  // 当模板文件修改时自动重新编译
                'debug' => \Scf\Core\App::isDevEnv(), // 开启调试模式
            ]);
            try {
                self::instance()->end($twig->render($status == 404 ? '404.html' : 'error.html', ['msg' => $output['message']]));
            } catch (LoaderError|RuntimeError|SyntaxError $e) {
                self::instance()->end($e->getMessage());
            }
        }
    }

    /**
     * 中断请求
     * @param $message
     * @param string $code
     * @param mixed $data
     * @param int $status
     * @return void
     */
    #[NoReturn] public static function interrupt($message, string $code = 'SERVICE_ERROR', mixed $data = '', int $status = 503): void {
        self::error($message, $code, $data, $status);
        self::instance()->stop($status);
    }

    /**
     * 中断请求
     * @param string $msg
     * @return void
     */
    #[NoReturn] public static function exit(string $msg = ''): void {
        self::instance()->end($msg);
        self::instance()->stop(200);
    }

    /**
     * URL重定向
     * @param string $url 重定向的URL地址
     * @param integer $time 重定向的等待时间（秒）
     * @param string $msg 重定向前的提示信息
     * @return void
     */
    public function redirect(string $url, int $time = 0, string $msg = ''): void {
        $str = <<<EOT
			<!DOCTYPE html>
<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<meta content="black" name="apple-mobile-web-app-status-bar-style">
		<meta content="telephone=no" name="format-detection">
		<meta content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=0" name="viewport">
		<title>正在跳转</title>
		<meta http-equiv='Refresh' content='{$time};URL={$url}'>
		<style>
			.content{
				margin-top: 200px;
				margin-left: 2%;
				width: 96%;
			}
			.chart{
				color:#fff;
				text-align: right;
				font-size: 12px;
				background:#006621;
				width:1%;
			}
			.message{
				color:#8d8d8d;
				text-align: center;
				width: 100%;
				font-size: 16px;
			}
		</style>
	</head>
	<body>
EOT;
        if ($time != 0) {
            $str .= <<<EOT
		<div class="content">
			<p class="message">{$msg}</p>
			<p class="chart">0%</p>
		</div>
		<script src="https://libs.baidu.com/jquery/1.9.0/jquery.js"></script>
		　<script>　
				 var percent = 0;
			 let totalTime = {$time} * 1000;
			 var timeOut = totalTime / 100;
			 count();
			 function count() {
				 percent = percent + 1;
				 $('.chart').css({width: percent + "%"});
				 $('.chart').html(percent + "%");
				 if (percent < 100)　 {
					 setTimeout(function () {
						 count();
					 }, timeOut);
					 //setTimeout("count()", 100);
				 } else {
					 //window.location = "{$url}";
				 }
			 }
		</script> 
EOT;
        }
        $str .= "</body></html>";
        //多行URL地址支持
        $url = str_replace(["\n", "\r"], '', $url);
        if (empty($msg)) {
            $msg = "系统将在{$time}秒之后自动跳转到{$url}！";
        }
        if (!headers_sent()) {
            $this->status(301);
            // redirect
            if (0 === $time) {
                $this->response->header('Location', $url, true);
            } else {
                $this->response->header('refresh', $time, true);
                $this->response->header('url', $url, true);
                $this->stop(301);
                //header("refresh:{$time};url={$url}");
                //echo($str);
                //echo($msg);
            }
        } else {
            //$str = "<meta http-equiv='Refresh' content='{$time};URL={$url}'>";
            $this->write($str);
            $this->stop(200);
        }
    }

    /**
     * @param $code
     * @return bool
     */
    public function status($code): bool {
        return $this->response->status($code);
    }

    public function setCookie($name, $val, $options = []): void {
        $cookie = session_get_cookie_params();
        $lifeTime = 0;
        if ($cookie['lifetime']) {
            $lifeTime = time() + $cookie['lifetime'];
        }
        $this->response->cookie($name, $val, $options['expire'] ?? $lifeTime, $options['path'] ?? $cookie['path'], $options['domain'] ?? $cookie['domain'], $cookie['secure'], $cookie['httponly']);
    }

    public function setHeader($key, $val, $format = true): void {
        $this->response->header(StringHelper::lower2camel($key), $val, $format);
    }

    public function json($data): void {
        $logger = ProcessLife::instance();
        Env::isDev() and $data['debug'] = $logger->requestDebugInfo();
        $this->setHeader('content-type', 'application/json;charset=utf-8', true);
        $this->setHeader('Server', 'scf-http-server', true);
        $this->end(json_encode($data, JSON_UNESCAPED_UNICODE));
    }

    public function write($content): void {
        $this->response->write($content);
    }

    public function end($content): void {
        $this->saveSlowLog();
        $this->response->end($content);
    }


    #[NoReturn] public function stop($code): void {
        $this->saveSlowLog();
        $this->isEnd = true;
        exit($code);
    }

    public function register(\Swoole\Http\Response $response): void {
        $this->response = $response;
    }

    protected function saveSlowLog(): void {
        $consume = App::instance()->consume();
        if ($consume > 3000) {
            $process = ProcessLife::instance()->requestDebugInfo();
            $logger = \Scf\Core\Log::instance();
            go(function () use ($consume, $process, $logger) {
                $log = [
                    'time' => date('Y-m-d H:i:s') . '.' . substr(Time::millisecond(), -3),
                    'path' => Request::server('path_info'),
                    'query' => Request::get()->pack(),
                    'post' => Request::post()->pack(),
                    'consume' => $process
                ];
                $logger->slow($log);
            });
        }
    }

}