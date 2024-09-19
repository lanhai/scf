<?php
namespace Scf\Database\Statistics\Dao;

use Scf\Database\Dao;
use Scf\Database\Tools\Calculator;

class StatisticdRecordDAO extends Dao {

    protected string $_dbName = "history";
    protected string $_table = "statistics_record";
    protected string $_primaryKey = "id";
    
    /**
     * @var int|Calculator|null id
     * @rule int|id数据格式错误
     */
    public int|Calculator|null $id;

    /**
     * @var int|Calculator|null 商户ID
     * @rule int|商户ID数据格式错误
     */
    public int|Calculator|null $mch_id;

    /**
     * @var ?string 模块
     * @rule string,max:50|模块长度不能大于50位
     */
    public ?string $scene;

    /**
     * @var ?string 模块数据ID
     * @rule string,max:32|模块数据ID长度不能大于32位
     */
    public ?string $data_id;

    /**
     * @var int|Calculator|null 访客uid
     * @rule int|访客uid数据格式错误
     */
    public int|Calculator|null $usr_id;

    /**
     * @var ?string 访客ID
     * @rule string,max:32|访客ID长度不能大于32位
     */
    public ?string $guest_id;

    /**
     * @var ?string 访客IP
     * @rule string,max:50|访客IP长度不能大于50位
     */
    public ?string $ip;

    /**
     * @var int|Calculator|null 访问时间
     * @rule int|访问时间数据格式错误
     */
    public int|Calculator|null $trigger_time;

    /**
     * @var int|Calculator|null 总访问次数
     * @rule int|总访问次数数据格式错误
     */
    public int|Calculator|null $trigger_times;

    /**
     * @var int|Calculator|null updated
     * @rule int|updated数据格式错误
     */
    public int|Calculator|null $updated;

    /**
     * @var ?string search_key
     * @rule string,max:32|search_key长度不能大于32位
     */
    public ?string $search_key;


}