<?php
declare(strict_types=1);
namespace Scf\Core\Traits { trait Singleton { public static function instance(): static { static $instance; return $instance ??= new static; } } }
namespace Scf\Core {
 class App { public static function isReady(): bool { return true; } public static function mount(): void {} }
 class Config { public static function server(): array { return []; } }
 class Console { public static function __callStatic($name, $args) {} }
 class Key {
  const RUNTIME_REDIS_QUEUE_MANAGER_PID='manager_pid', RUNTIME_SUBPROCESS_MANAGER_GENERATION='generation', COUNTER_REDIS_QUEUE_PROCESS='counter', RUNTIME_REDIS_QUEUE_WORKER_STATE='state', RUNTIME_REDIS_QUEUE_WORKER_PID='pid', RUNTIME_GATEWAY_STARTUP_SUMMARY_PENDING='summary';
 }
}
namespace Scf\Core\Table {
 class Runtime { use \Scf\Core\Traits\Singleton; public array $data=[]; public function get($key) { return $this->data[$key]??null; } public function set($key,$value): void { $this->data[$key]=$value; } }
 class Counter { use \Scf\Core\Traits\Singleton; public function get($key): int { return 1; } }
}
namespace Scf\Cache { class Redis { public static function pool(): \stdClass { return new \stdClass; } } }
namespace Scf\Util {
 class File { public static function write($path,$value): void { file_put_contents($path,(string)$value); } }
 class MemoryMonitor { public static function start($key): void { \Swoole\Timer::tick(1000, static function() {}); } public static function stop(): void {} }
}
namespace {
 if (!extension_loaded('swoole')) throw new RuntimeException('Swoole is required');
 require dirname(__DIR__).'/src/Server/Task/RQueue.php';
 $base=sys_get_temp_dir().'/scf-native-queue-'.getmypid();
 define('SERVER_QUEUE_MANAGER_PID_FILE',$base);
 class QueueBootProbe extends \Scf\Server\Task\RQueue {
  public function watch(int $mc=32, string $managerGeneration=''): int {
   file_put_contents(SERVER_QUEUE_MANAGER_PID_FILE.'.result', json_encode(['cid'=>\Swoole\Coroutine::getCid(),'lease'=>self::workerLockIsHeld(getmypid(),'qa-native-boot')]));
   \Swoole\Timer::after(30, function(): void { $this->shutdownRuntime(); });
   return 0;
  }
 }
 $runtime=\Scf\Core\Table\Runtime::instance();
 $runtime->set('manager_pid',getmypid());$runtime->set('generation','qa-generation');
 $process=QueueBootProbe::startProcess('qa-native-boot','qa-generation');
 if (!$process) throw new RuntimeException('Native child did not start');
 $deadline=microtime(true)+2;$done=false;
 try {
  do { $exit=\Swoole\Process::wait(false); if ($exit) { $done=true;break; } usleep(10000); } while(microtime(true)<$deadline);
  if (!$done) throw new RuntimeException('Queue child remained alive without entering the consumer coroutine');
  $result=is_file($base.'.result')?json_decode(file_get_contents($base.'.result'),true):[];
  if (($result['cid']??-1)<=0 || empty($result['lease'])) throw new RuntimeException('Consumer must run in native coroutine while retaining worker lease');
  if (QueueBootProbe::workerLockIsHeld()) throw new RuntimeException('Worker lease must release after child exit');
  echo "PASS actual RQueue process: coroutine starts after timer setup, consumer holds lease, channel shutdown clears timers and releases lease\n";
 } finally {
  if (!$done) { \Swoole\Process::kill($process->pid,SIGTERM);\Swoole\Process::wait(); }
  foreach([$base,$base.'.result',$base.'.worker.lock',$base.'.worker.lifecycle.lock'] as $file) if(is_file($file)) unlink($file);
 }
}
