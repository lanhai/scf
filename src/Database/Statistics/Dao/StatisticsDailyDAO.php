<?php
namespace Scf\Database\Statistics\Dao;

use Scf\Database\Dao;
use Scf\Database\Tools\Calculator;

class StatisticsDailyDAO extends Dao {

    protected string $_dbName = "statistics";
    protected string $_table = "daily";
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
     * @var int|Calculator|null 统计日期
     * @rule int|统计日期数据格式错误
     */
    public int|Calculator|null $day;

    /**
     * @var int|Calculator|null 统计小时段
     * @rule int|统计小时段数据格式错误
     */
    public int|Calculator|null $hour;

    /**
     * @var int|Calculator|null 独立用户访问数
     * @rule int|独立用户访问数数据格式错误
     */
    public int|Calculator|null $uv;

    /**
     * @var int|Calculator|null 页面访问次数
     * @rule int|页面访问次数数据格式错误
     */
    public int|Calculator|null $pv;

    /**
     * @var float|Calculator|null 相关数值
     * @rule float|相关数值数据格式错误
     */
    public float|Calculator|null $value;

    /**
     * @var int|Calculator|null 最近一次更新时间
     * @rule int|最近一次更新时间数据格式错误
     */
    public int|Calculator|null $updated;

    /**
     * @var ?string search_key
     * @rule string,max:32|search_key长度不能大于32位
     */
    public ?string $search_key;


}