<?php

namespace Scf\Database\Model;

use Scf\Database\Dao;
use Scf\Helper\JsonHelper;

class Config extends Dao {
    /**
     * @var string id
     * @rule string,max:32|id长度不能大于32位
     */
    public string $id;

    /**
     * @var ?string value
     * @rule string|value数据格式错误
     */
    public ?string $value;

    /**
     * @var int 缓存时间
     */
    protected int $cacheLifeTime = 3600;

    /**
     * 获取所有系统配置
     * @return array
     */
    public static function getAll(): array {
        $list = self::select()->all();
        $config = [];
        if ($list) {
            foreach ($list as $item) {
                $config[$item['id']] = JsonHelper::is($item['value']) ? JsonHelper::recover($item['value']) : $item['value'];
            }
        }
        return $config;
    }

    /**
     * 获取制定设置项的值
     * @param string $key 字段名称
     * @return mixed 配置值
     */
    public static function get(string $key): mixed {
        $ar = self::unique($key);
        if ($ar->notExist()) {
            return null;
        }
        $data = $ar->toArray();
        if ($data) {
            if (JsonHelper::is($data['value'])) {
                return JsonHelper::recover($data['value']);
            }
            return $data['value'];
        }
        return null;
    }

    /**
     * 更新
     * @param $id
     * @param $value
     * @return bool
     */
    public static function set($id, $value): bool {
        $exist = self::get($id);
        if ($exist === "" || is_null($exist)) {
            $ar = self::factory();
            $ar->id = $id;
            $ar->value = $value;
            return $ar->save();
        }
        $ar = self::unique($id);
        if (is_array($value)) {
            $value = JsonHelper::toJson($value);
        }
        $ar->value = $value;
        return $ar->save();
    }
}