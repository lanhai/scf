<?php

namespace Scf\Command\Handler;

use ErrorException;
use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Command\Color;
use Scf\Server\Manager;
use Scf\Server\Struct\Node;
use Swlib\Http\Exception\RequestException;
use Swlib\SaberGM;
use Swoole\Coroutine;
use Swoole\Coroutine\Barrier;
use Swoole\Event;
use Swoole\Exception;
use Swoole\Runtime;
use function Swoole\Coroutine\run;

class NodeManager {
    /**
     * @var array 节点列表
     */
    protected array $nodes = [];

    /**
     * @throws ErrorException
     */
    public function run(): string {
        Runtime::enableCoroutine();
        $this->getNodes();
        $this->status();
        return $this->ready();
    }

    /**
     * 节点状态
     * @return void
     */
    protected function status(): void {
        run(function () {
            $version = Updater::instance()->getRemoteVersion();
            Console::line();
            Console::write("最新APP版本:" . $version['app']['version']);
            //Console::write("最新SCF版本:" . (!is_null($version['scf']) ? $version['scf']['version'] ?? '未知' : '未知'));
        });
        Console::write('------------------------------------------------------------------------------------------');
        Console::write(' ID   节点类型   版本      Host             启动时间         心跳时间        重启  状态');
        Console::write('------------------------------------------------------------------------------------------');
        if (!$this->nodes) {
            Console::write('暂无节点');
        } else {
            foreach ($this->nodes as $k => $n) {
                if (!$n) {
                    continue;
                }
                $node = Node::factory($n);
                $status = (time() - $node->heart_beat) <= 5;
                Console::write("【" . ($k + 1) . "】【" . $node->role . "】 【v" . $node->app_version . "】 " . $node->ip . ":" . $node->port . "  " . date('m-d H:i:s', $node->started) . "   " . date('m-d H:i:s', $node->heart_beat) . "   " . $node->restart_times . "    " . ($status ? Color::green('活跃') : Color::yellow('停止')));
            }
        }
        Console::write('------------------------------------------------------------------------------------------');
    }

    protected function cmd($input): string {
        $arr = explode(" ", $input);
        $cmd = $arr[0] ?? 'status';
        $nodeId = null;
        if ($cmd !== 'status' && $cmd !== 'reloadall' && $cmd !== 'logs' && $cmd !== 'update') {
            $nodeId = $arr[1] ?? null;
            if (!$nodeId) {
                Console::write('指令错误,缺少节点ID,示范:reload 1');
                return $this->ready();
            }
            if (!isset($this->nodes[$nodeId - 1])) {
                Console::write('指令错误,服务器节点不存在');
                return $this->ready();
            }
        }
        Console::line();
        switch ($cmd) {
            case 'update':
                Console::write('正在更新···');
                $this->getNodes();
                $this->update();
                Console::line();
//                foreach ($this->nodes as $key => $node) {
//                    $this->send($key, 'update');
//                }
                Console::write('等待刷新···');
                sleep(10);
                break;
            case 'reloadall':
                Console::write('正在重启···');
                Console::line();
                foreach ($this->nodes as $key => $node) {
                    $this->send($key, 'reload');
                }
                sleep(1);
                break;
            case 'logs':
                Console::write('正在监听所有节点控制台日志');
                $this->getNodes();
                $this->logSubscribe();
                Console::line();
                break;
            case 'reload':
                Console::write('正在重启···');
                Console::line();
                $this->send($nodeId - 1, 'reload');
                sleep(1);
                break;
            case 'message':
                $msg = $arr[2] ?? null;
                if (!$msg) {
                    Console::write('请输入要发送的内容,示范:message 1 hello');
                    $this->ready();
                }
                $this->send($nodeId - 1, $msg);
                Console::write('消息已发送:' . $msg);
                break;
        }
        return $this->run();
    }

    /**
     * 日志订阅
     * @return void
     */
    protected function logSubscribe() {
        run(function () {
            foreach ($this->nodes as $node) {
                if (!$node) {
                    continue;
                }
                $node = Node::factory($node);
                if (time() - $node->heart_beat >= 3) {
                    continue;
                }
                try {
                    if (SERVER_HOST_IS_IP) {
                        $socketHost = $node->ip . ':' . $node->socketPort;
                    } else {
                        $socketHost = $node->socketPort . '.' . SERVER_HOST;
                    }
                    $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
                    Coroutine::create(function () use ($websocket, $node) {
                        $websocket->push('log_subscribe');
                        while (true) {
                            $reply = $websocket->recv(1);
                            if ($reply) {
                                if (!$reply->data) {
                                    Console::log("【" . $node->ip . "】日志监听连接已断开", false);
                                    break;
                                } else {
                                    Console::log("【" . $node->ip . "】" . Color::green($reply), false);
                                }
                            }
                            Coroutine::sleep(0.1);
                        }
                    });
                } catch (RequestException $exception) {
                    Console::log(Color::red("【" . $socketHost . "】" . "连接失败:" . $exception->getMessage()), false);
                }
            }
        });
    }

    /**
     * 更新
     * @return void
     */
    protected function update(): void {
        run(function () {
            foreach ($this->nodes as $node) {
                if (!$node) {
                    continue;
                }
                $node = Node::factory($node);
                if (time() - $node->heart_beat >= 3) {
                    continue;
                }
                try {
                    if (SERVER_HOST_IS_IP) {
                        $socketHost = $node->ip . ':' . $node->socketPort;
                    } else {
                        $socketHost = $node->socketPort . '.' . SERVER_HOST;
                    }
                    $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
                    Coroutine::create(function () use ($websocket, $node) {
                        $websocket->push('update');
                        while (true) {
                            $reply = $websocket->recv(1);
                            if ($reply) {
                                if (!$reply->data) {
                                    Console::log("【" . $node->ip . "】升级完成", false);
                                    break;
                                } else {
                                    Console::log("【" . $node->ip . "】" . Color::green($reply), false);
                                }
                            }
                            Coroutine::sleep(0.1);
                        }
                    });
                } catch (RequestException $exception) {
                    Console::log(Color::red("【" . $socketHost . "】" . "连接失败:" . $exception->getMessage()), false);
                }
            }
        });
        Event::wait();
    }

    /**
     * 向所有节点发送更新到指定版本命令
     * @throws Exception
     */
    public function appointUpdate($type, $version): Result {
        $this->nodes = Manager::instance()->getServers();
        if (!$this->nodes) {
            return Result::error('节点获取失败');
        }
        $barrier = Barrier::make();
        $count = 0;
        foreach ($this->nodes as $node) {
            if (!$node) {
                continue;
            }
            Coroutine::create(function () use ($barrier, $node, $type, $version, &$count) {
                $node = Node::factory($node);
                if (time() - $node->heart_beat >= 3) {
                    return;
                }
                try {
                    if (SERVER_HOST_IS_IP) {
                        $socketHost = $node->ip . ':' . $node->socketPort;
                    } else {
                        $socketHost = $node->socketPort . '.' . SERVER_HOST;
                    }
                    $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
                    $websocket->push('appoint_update:' . $type . '|' . $version);
                    while (true) {
                        $reply = $websocket->recv(1);
                        if ($reply) {
                            if (!$reply->data) {
                                $count++;
                                Console::log("【" . $node->ip . "】升级完成", false);
                                break;
                            } else {
                                Console::log("【" . $node->ip . "】" . Color::green($reply), false);
                            }
                        }
                        Coroutine::sleep(0.5);
                    }

                } catch (RequestException $exception) {
                    Console::log(Color::red("【" . $socketHost . "】" . "连接失败:" . $exception->getMessage()), false);
                }
            });
        }
        Barrier::wait($barrier);
        return Result::success($count);
    }

    /**
     * 发送socket消息
     * @param $num
     * @param $msg
     * @return void
     */
    protected function send($num, $msg): void {
        $node = Node::factory($this->nodes[$num]);
        run(function () use ($node, $msg) {
            $websocket = SaberGM::websocket('ws://' . $node->ip . ':' . $node->socketPort . '?username=manager&password=' . md5(App::authKey()));
            Coroutine::create(function () use ($websocket, $msg, $node) {
                $websocket->push($msg);
                while (true) {
                    $reply = $websocket->recv(1);
                    if ($reply) {
                        break;
                    }
                    Coroutine::sleep(1);
                }
                Console::log("【" . $node->id . "】回复:" . Color::green($reply));
            });
        });
    }

    protected function ready(): string {
        $input = Console::input('请输入要执行的指令(status|reload|message|reloadall|logs|update) 示范:restart 1');
        return $this->cmd($input);
    }


    public function getNodes(): void {
        run(function () {
            $this->nodes = Manager::instance()->getServers();
        });
    }
}