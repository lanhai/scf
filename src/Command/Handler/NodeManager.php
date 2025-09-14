<?php

namespace Scf\Command\Handler;

use ErrorException;
use Scf\App\Updater;
use Scf\Core\App;
use Scf\Core\Console;
use Scf\Core\Result;
use Scf\Command\Color;
use Scf\Helper\JsonHelper;
use Scf\Server\Manager;
use Scf\Server\Struct\Node;
use Swlib\Http\Exception\RequestException;
use Swlib\SaberGM;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Runtime;
use Swoole\Timer;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Output\ConsoleOutput;

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
        $output = new ConsoleOutput();
        $table = new Table($output);
        $rows = [];
        foreach ($this->nodes as $k => $n) {
            if (!$n) {
                continue;
            }
            $n['framework_build_version'] = $n['framework_build_version'] ?? '--';
            $n['framework_update_ready'] = $n['framework_update_ready'] ?? false;
            $node = Node::factory($n);
            $status = (time() - $node->heart_beat) <= 5;
            if (filter_var($node->ip, FILTER_VALIDATE_IP) !== false) {
                $socketHost = $node->ip . ':' . $node->port;
            } else {
                $socketHost = $node->ip;
            }
            $rows[] = [
                $k + 1,
                $node->role,
                $node->scf_version,
                $node->app_version,
                $socketHost,
                date('m-d H:i:s', $node->started),
                date('m-d H:i:s', $node->heart_beat),
                $node->restart_times . '次',
                $status ? Color::green('活跃') : Color::yellow('停止')
            ];
        }
        $table
            ->setHeaders(['ID', '节点类型', '框架笨笨', '核心版本', 'Host', '启动时间', '心跳时间', '重启', '状态'])
            ->setRows($rows);
        $table->render();
    }

    /**
     * @throws ErrorException
     */
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
    protected function logSubscribe(): void {
        run(function () {
            foreach ($this->nodes as $node) {
                if (!$node) {
                    continue;
                }
                $node['framework_build_version'] = $node['framework_build_version'] ?? '--';
                $node['framework_update_ready'] = $node['framework_update_ready'] ?? false;
                $node = Node::factory($node);
                if (time() - $node->heart_beat >= 3) {
                    continue;
                }
                try {
                    Coroutine::create(function () use ($node) {
                        if (filter_var($node->ip, FILTER_VALIDATE_IP) !== false) {
                            $socketHost = $node->ip . ':' . $node->socketPort;
                        } else {
                            $socketHost = $node->socketPort . '.' . $node->ip . '/dashboard.socket';
                        }
                        $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
                        $websocket->push('log_subscribe');
                        // 使用 Swoole 定时器定期接收消息
                        while (true) {
                            $reply = $websocket->recv(1);
                            if ($reply) {
                                if (!$reply->data) {
                                    Console::info("【NODE-" . $node->ip . "】日志监听连接已断开", false);
                                    break;
                                } else {
                                    Console::info("【NODE-" . $node->ip . "】" . Color::green($reply), false);
                                }
                            }
                            Coroutine::sleep(0.1);
                        }
                    });
                } catch (RequestException $exception) {
                    Console::error("【NODE-" . $node->ip . "】" . "连接失败:" . $exception->getMessage(), false);
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
                $node['framework_build_version'] = $node['framework_build_version'] ?? '--';
                $node['framework_update_ready'] = $node['framework_update_ready'] ?? false;
                $node = Node::factory($node);
                if (time() - $node->heart_beat >= 3) {
                    continue;
                }
                try {
                    if (SERVER_HOST_IS_IP) {
                        $socketHost = $node->ip . ':' . $node->socketPort;
                    } else {
                        $socketHost = $node->socketPort . '.' . $node->ip . '/dashboard.socket';
                    }
                    $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
                    Coroutine::create(function () use ($websocket, $node) {
                        $websocket->push('update');
                        while (true) {
                            $reply = $websocket->recv(1);
                            if ($reply) {
                                if (!$reply->data) {
                                    Console::info("【NODE-" . $node->ip . "】升级完成", false);
                                    break;
                                } else {
                                    Console::info("【NODE-" . $node->ip . "】" . Color::green($reply), false);
                                }
                            }
                            Coroutine::sleep(0.1);
                        }
                    });
                } catch (RequestException $exception) {
                    Console::error("【NODE-" . $node->ip . "】" . "连接失败:" . $exception->getMessage(), false);
                }
            }
        });
        Event::wait();
    }

    /**
     * 向所有节点发送更新到指定版本命令
     */
    public function appointUpdate($type, $version): Result {
        $socket = Manager::instance()->getMasterSocketConnection();
        $socket->push(JsonHelper::toJson(['event' => 'appoint_update', 'data' => ['type' => $type, 'version' => $version]]));
        $reply = $socket->recv(30);
        if ($reply === false) {
            $socket->close();
            return Result::error('向master节点发送指令失败');
        }
        return Result::success();
//        $this->nodes = Manager::instance()->getServers();
//        if (!$this->nodes) {
//            return Result::error('节点获取失败');
//        }
//        $barrier = Barrier::make();
//        $count = 0;
//        foreach ($this->nodes as $node) {
//            if (!$node) {
//                continue;
//            }
//            Coroutine::create(function () use ($barrier, $node, $type, $version, &$count) {
//                $node['framework_build_version'] = $node['framework_build_version'] ?? '--';
//                $node['framework_update_ready'] = $node['framework_update_ready'] ?? false;
//                $node = Node::factory($node);
//                if (time() - $node->heart_beat >= 30) {
//                    return;
//                }
//                try {
//                    if (SERVER_HOST_IS_IP) {
//                        $socketHost = $node->ip . ':' . $node->socketPort;
//                    } else {
//                        $socketHost = $node->socketPort . '.' . $node->ip . '/dashboard.socket';
//                    }
//                    $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
//                    $websocket->push('appoint_update:' . $type . '|' . $version);
//                    while (true) {
//                        $reply = $websocket->recv(1);
//                        if ($reply) {
//                            if (!$reply->data) {
//                                $count++;
//                                break;
//                            } else {
//                                Console::info("【NODE-" . $node->ip . "】" . Color::green($reply), false);
//                            }
//                        }
//                        Coroutine::sleep(0.5);
//                    }
//
//                } catch (RequestException $exception) {
//                    Console::error("【node-" . $node->ip . "】" . "连接失败:" . $exception->getMessage(), false);
//                }
//            });
//        }
//        Barrier::wait($barrier);
//        return Result::success($count);
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
            if (SERVER_HOST_IS_IP) {
                $socketHost = $node->ip . ':' . $node->socketPort;
            } else {
                $socketHost = $node->socketPort . '.' . $node->ip . '/dashboard.socket';
            }
            $websocket = SaberGM::websocket('ws://' . $socketHost . '?username=manager&password=' . md5(App::authKey()));
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

    /**
     * @throws ErrorException
     */
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