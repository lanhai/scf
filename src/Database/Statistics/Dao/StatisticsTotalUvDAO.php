<?php
namespace Scf\Database\Statistics\Dao;

use Scf\Database\Dao;
use Scf\Database\Tools\Calculator;

class StatisticsTotalUvDAO extends Dao {

    protected string $_dbName = "statistics";
    protected string $_table = "total_uv";
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
     * @var ?string 用户标识
     * @rule string,max:32|用户标识长度不能大于32位
     */
    public ?string $user_key;

    /**
     * @var int|Calculator|null 创建时间
     * @rule int|创建时间数据格式错误
     */
    public int|Calculator|null $created;


}