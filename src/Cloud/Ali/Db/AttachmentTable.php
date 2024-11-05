<?php
namespace Scf\Cloud\Ali\Db;

use Scf\Database\Dao;
use Scf\Database\Tools\Calculator;

class AttachmentTable extends Dao {

    protected string $_dbName = "default";
    protected string $_table = "attachment";
    protected string $_primaryKey = "id";
    
    /**
     * @var int|Calculator|null id
     * @rule int|id数据格式错误
     */
    public int|Calculator|null $id;

    /**
     * @var ?string 服务器ID
     * @rule string,max:50|服务器ID长度不能大于50位
     */
    public ?string $oss_server;

    /**
     * @var ?string 文件仓库
     * @rule string,max:50|文件仓库长度不能大于50位
     */
    public ?string $oss_bucket;

    /**
     * @var ?string 文件名称
     * @rule string,max:255|文件名称长度不能大于255位
     */
    public ?string $oss_object;

    /**
     * @var ?string 文件类型
     * @rule string,max:50|文件类型长度不能大于50位
     */
    public ?string $file_type;

    /**
     * @var ?string 文件后缀名
     * @rule string,max:10|文件后缀名长度不能大于10位
     */
    public ?string $file_ext;

    /**
     * @var ?string 文件源名称
     * @rule string,max:255|文件源名称长度不能大于255位
     */
    public ?string $file_original_name;

    /**
     * @var int|Calculator|null 文件大小
     * @rule int|文件大小数据格式错误
     */
    public int|Calculator|null $file_size;

    /**
     * @var ?string 文件所属应用
     * @rule string,max:50|文件所属应用长度不能大于50位
     */
    public ?string $app;

    /**
     * @var ?string 应用数据id
     * @rule string,max:50|应用数据id长度不能大于50位
     */
    public ?string $app_id;

    /**
     * @var int|Calculator|null 文件状态0未审核
     * @rule int|文件状态0未审核只能为数字格式
     */
    public int|Calculator|null $status;

    /**
     * @var int|Calculator|null 审核人UID
     * @rule int|审核人UID数据格式错误
     */
    public int|Calculator|null $verify_uid;

    /**
     * @var ?string 创建场景 1:后台上传 2:前台上传 3:系统下载
     * @rule string,max:10|创建场景 1:后台上传 2:前台上传 3:系统下载长度不能大于10位
     */
    public ?string $created_scene;

    /**
     * @var int|Calculator|null 创建者UID
     * @rule int|创建者UID数据格式错误
     */
    public int|Calculator|null $created_uid;

    /**
     * @var int|Calculator|null 创建时间
     * @rule int|创建时间数据格式错误
     */
    public int|Calculator|null $created_at;

    /**
     * @var int|Calculator|null 删除时间
     * @rule int|删除时间数据格式错误
     */
    public int|Calculator|null $deleted_at;

    /**
     * @var int|Calculator|null 删除者用户ID
     * @rule int|删除者用户ID数据格式错误
     */
    public int|Calculator|null $deleted_uid;


}