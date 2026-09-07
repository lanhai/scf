<?php
declare(strict_types=1);
namespace Scf\Core { class Struct {} }
namespace {
    define('DBS_MASTER', 1); define('DBS_SLAVE', 2);
    require __DIR__.'/query_builder_in_placeholder_regression.php';
    require dirname(__DIR__).'/src/Database/Tools/WhereBuilder.php';
    require dirname(__DIR__).'/src/Database/Dao.php';
    class AtomicConnection extends QueryBuilderInPlaceholderProbe {
        public static array $queries=[];
        public static array $row=['id'=>8,'status'=>1,'token'=>12];
        public static bool $fail=false;
        private int $changed=0;
        public function updates(array $data): \Scf\Database\IConnection {
            if(self::$fail) throw new RuntimeException('database failure');
            [$sql,$values]=$this->build('UPDATE',$data);
            self::$queries[]=[$sql,$values];
            $this->changed=(self::$row['id']===8 && self::$row['status']===1 && self::$row['token']===12)?1:0;
            if($this->changed) self::$row=array_replace(self::$row,$data);
            return $this;
        }
        public function rowCount(): int { return $this->changed; }
        public function get(): array { throw new RuntimeException('CAS must never SELECT primary keys before UPDATE'); }
    }
    class AtomicDao extends \Scf\Database\Dao {
        public static array $invalidated=[];
        public function __construct() { $this->_table='jobs'; }
        protected function connection(int $actor=0,bool $resetParams=true): \Scf\Database\IConnection {
            if($actor!==DBS_MASTER) throw new RuntimeException('Writes must use master');
            $c=(new AtomicConnection())->table('jobs');
            $where=$this->_where->build();
            return $c->where($where['sql'], ...$where['match']);
        }
        protected function deleteArCache(int|array|string $id): void { self::$invalidated[]=$id; }
    }
    $where=['id'=>8,'status'=>1,'token'=>12];
    if(AtomicDao::compareAndUpdate($where,['status'=>2])!==1) throw new RuntimeException('First transition failed');
    [$sql,$values]=AtomicConnection::$queries[0];
    foreach(['id','status','token'] as $field) if(!str_contains($sql,'`'.$field.'`')) throw new RuntimeException('Missing predicate '.$field);
    if($values!==[2,8,1,12]) throw new RuntimeException('Incorrect bindings: '.json_encode($values));
    if(AtomicDao::compareAndUpdate($where,['status'=>-1])!==0) throw new RuntimeException('Stale state accepted');
    if(AtomicDao::$invalidated!==[8]) throw new RuntimeException('Cache must invalidate only changed primary key');
    try { AtomicDao::compareAndUpdate(['status'=>1],['status'=>2]); throw new RuntimeException('missing key allowed'); } catch(InvalidArgumentException $expected) {}
    AtomicConnection::$fail=true;
    try { AtomicDao::compareAndUpdate($where,['status'=>2]); throw new RuntimeException('failure swallowed'); } catch(RuntimeException $error) { if($error->getMessage()!=='database failure') throw $error; }
    echo "PASS actual Dao CAS and SQL builder: no preliminary SELECT, all predicates/bindings retained, stale writes rejected, cache invalidated and DB errors propagated\n";
}
