<?php

namespace Scf\Service\Struct;

use Scf\Core\Struct;

class QueueStruct extends Struct {

    /**
     * @var ?string id
     */
    public ?string $id;

    /**
     * @var ?string 执行脚本
     * @rule string,max:100|执行脚本长度不能大于100位
     */
    public ?string $handler;

    /**
     * @var ?array data
     * @rule array|业务数据格式错误
     */
    public ?array $data;

    /**
     * @var ?int created
     * @rule int,max:9999999999|created不能大于9999999999
     */
    public ?int $created;
    /**
     * @var ?int 是否重试
     */
    public ?int $retry;
    /**
     * @var ?int 当前执行次数
     */
    public ?int $try_times;
    /**
     * @var ?int 执行次数限制
     */
    public ?int $try_limit;

    /**
     * @var ?int 更新时间
     */
    public ?int $updated;
    /**
     * @var ?int
     */
    public ?int $next_try;
    /**
     * @var ?int finished
     * @rule int,max:9999999999|finished不能大于9999999999
     */
    public ?int $finished;

    /**
     * @var ?int status
     * @rule int,max:9|status不能大于9
     */
    public ?int $status;

    /**
     * @var ?string remark
     * @rule string,max:255|remark长度不能大于255位
     */
    public ?string $remark;
    /**
     * @var ?array 运行结果
     * @rule array|业务数据格式错误
     */
    public ?array $result;
    /**
     * @var ?int
     */
    public ?int $start;
    /**
     * @var ?int
     */
    public ?int $end;
    /**
     * @var ?int
     */
    public ?int $duration;

}