<?php
declare(strict_types=1);

/** Verify opt-in fresh AR reads without changing legacy caching or save snapshots. */
namespace Scf\Core {
    class Struct {
        protected array $data = [];
        public function install(array $row): bool { $this->data = $row; return true; }
        public function asArray(): array { return $this->data; }
    }
}
namespace Scf\Component {
    class Cache {
        public static array $rows = [];
        public static int $reads = 0;
        public static function instance(): self { return new self(); }
        public function get(string $key): mixed { self::$reads++; return self::$rows[$key] ?? false; }
        public function set(string $key, mixed $value, int $ttl): bool { self::$rows[$key] = $value; return true; }
    }
}
namespace {
    define('DBS_MASTER', 1); define('DBS_SLAVE', 2);
    require dirname(__DIR__).'/src/Database/Tools/WhereBuilder.php';
    require dirname(__DIR__).'/src/Database/Exception/NullAR.php';
    require dirname(__DIR__).'/src/Database/Dao.php';
    class Video extends \Scf\Database\Dao {
        protected string $_table = 'videos';
        public static ?array $row = ['id'=>1, 'first_review_status'=>0];
        public static array $reads = [];
        public function first(bool $format = true): ?array { self::$reads[] = $this->actor; return self::$row; }
        public function queryPrimaryVal(): mixed { throw new RuntimeException('Fresh WHERE reads must not take the cached primary-key lookup path'); }
        public function snapshotForTest(): ?array { return $this->snapshot; }
        public function primaryForTest(): int|string|null { return $this->getPrimaryVal(); }
        public static function masterWhere(): self { return self::select()->setActor(DBS_MASTER)->where(['id'=>1]); }
    }
    function check(bool $value, string $message): void { if (!$value) throw new RuntimeException($message); }
    $old = Video::unique(1)->ar();
    check($old->exist() && $old->asArray()['first_review_status']===0, 'Legacy cold-cache path loads the row');
    check(Video::$reads === [DBS_SLAVE], 'Legacy default data source remains unchanged');
    Video::$row['first_review_status'] = 1;
    check(Video::unique(1, DBS_MASTER)->ar()->asArray()['first_review_status']===0, 'Legacy cached master reads retain their documented default');
    check(count(Video::$reads)===1, 'Legacy cache hit avoids the SQL query');
    $cached = \Scf\Component\Cache::$rows;
    $cacheReads = \Scf\Component\Cache::$reads;
    foreach ([Video::unique(1, DBS_MASTER), Video::masterWhere()] as $query) {
        $fresh = $query->ar(false);
        check($fresh->exist() && $fresh->asArray()===Video::$row, 'Fresh primary-key and WHERE reads return current SQL state');
        check($fresh->snapshotForTest()===Video::$row, 'Fresh AR creates the same differential-save snapshot');
        check($fresh->primaryForTest()===1, 'Fresh WHERE reads retain the primary key for later updates and deletes');
    }
    check(Video::$reads === [DBS_SLAVE, DBS_MASTER, DBS_MASTER], 'Fresh reads retain the explicitly chosen master');
    check(\Scf\Component\Cache::$reads===$cacheReads && \Scf\Component\Cache::$rows===$cached, 'Fresh reads neither consult nor populate cache');
    Video::$row = null;
    check(Video::unique(1, DBS_MASTER)->ar(false)->notExist(), 'Missing row cannot be resurrected by cached data');
    echo "PASS DAO fresh/cached reads, master selection, missing rows and save snapshots\n";
}
