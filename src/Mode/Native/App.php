<?php

namespace Scf\Mode\Native;

use Scf\Core\Traits\Singleton;
use Scf\Helper\JsonHelper;
use Scf\Server\Table\Runtime;
use Scf\Server\Native;

abstract class App {
    use Singleton;

    protected Native $server;

    abstract function boot();

    /**
     * 初始化
     * @param Native $server
     * @return App
     */
    public function init(Native $server): static {
        //spl_autoload_register([__CLASS__, 'autoload'], true);
        $this->server = $server;
        return $this;
    }

    protected function window(): Window {
        return Window::bind($this);
    }

    public function createWidth(Window $window): void {
        $this->sendCommand('/window/create', ['width' => $window->getWidth(), 'height' => $window->getHeight(), 'index' => $window->getIndex()]);
    }

    protected function sendNotification($title, $body): void {
        $this->sendCommand('/notification/send', ['title' => $title, 'body' => $body]);
    }

    protected function sendMessage($message): void {
        if ($fd = Runtime::instance()->get('_ICP_MAIN_CLIENT_ID_')) {
            $this->server->send($fd, $this->message($message));
        }

    }

    protected function sendCommand($command, $content): void {
        if ($fd = Runtime::instance()->get('_ICP_MAIN_CLIENT_ID_')) {
            $this->server->send($fd, $this->command($command, $content));
        }

    }

    public static function message($msg): bool|string {
        return JsonHelper::toJson(['type' => 'message', 'content' => $msg]);
    }

    public static function response($data, $requestId): bool|string {
        return JsonHelper::toJson(['type' => 'response', 'content' => $data, 'request_id' => $requestId]);
    }

    public static function command($cmd, $content): bool|string {
        return JsonHelper::toJson(['type' => 'command', 'content' => $content, 'command' => $cmd]);
    }
}
