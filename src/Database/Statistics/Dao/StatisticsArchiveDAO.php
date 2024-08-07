<?php
namespace Scf\Database\Statistics\Dao;

use Scf\Database\Dao;
use Scf\Database\Tools\Calculator;

class StatisticsArchiveDAO extends Dao {

    protected string $_dbName = "statistics";
    protected string $_table = "archive";
    protected string $_primaryKey = "id";
    
    /**
     * @var int|Calculator|null id
     * @rule int|id数据格式错误
     */
    public int|Calculator|null $id;

    /**
     * @var ?string search_key
     * @rule string,max:32|search_key长度不能大于32位
     */
    public ?string $search_key;

    /**
     * @var int|Calculator|null 数据类型 1:count 2:sum
     * @rule int|数据类型 1:count 2:sum只能为数字格式
     */
    public int|Calculator|null $type;

    /**
     * @var int|Calculator|null 时间类型 1:minute 2:hour 3:day 4:month
     * @rule int|时间类型 1:minute 2:hour 3:day 4:month只能为数字格式
     */
    public int|Calculator|null $date_type;

    /**
     * @var ?string db
     * @rule string,max:255|db长度不能大于255位
     */
    public ?string $db;

    /**
     * @var ?string where
     * @rule string,max:255|where长度不能大于255位
     */
    public ?string $where;

    /**
     * @var ?string date
     * @rule string,max:255|date长度不能大于255位
     */
    public ?string $date;

    /**
     * @var float|Calculator|null value
     * @rule float|value数据格式错误
     */
    public float|Calculator|null $value;

    /**
     * @var int|Calculator|null day
     * @rule int|day数据格式错误
     */
    public int|Calculator|null $day;

    /**
     * @var int|Calculator|null hour
     * @rule int|hour数据格式错误
     */
    public int|Calculator|null $hour;

    /**
     * @var ?string key
     * @rule string,max:32|key长度不能大于32位
     */
    public ?string $key;

    /**
     * @var int|Calculator|null 创建时间
     * @rule int|创建时间数据格式错误
     */
    public int|Calculator|null $updated;


}