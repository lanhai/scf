<?php

namespace Scf\Service;

use Overtrue\Pinyin\Pinyin;
use Scf\Cache\Redis;
use Scf\Client\Http;
use Scf\Core\Result;
use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\Singleton;
use Scf\Util\Date;

class Amap {
    use ComponentTrait, Singleton;

    protected string $ak = '08c5b12eb20674b61c89a4744d7e4551';
    protected string $districtGateway = 'https://restapi.amap.com/v3/config/district';
    // 定义排序函数

    /**
     * 根据首字母排序
     * @param $a
     * @param $b
     * @return int
     */
    public static function compareByTextInitial($a, $b): int {
//        $textA = Pinyin::abbr(mb_substr($a['text'], 0, 1));
//        $textB = Pinyin::abbr(mb_substr($b['text'], 0, 1));
        // 使用strcasecmp进行字母不区分大小写的比较
        return strcasecmp($a['pinyin'], $b['pinyin']);
    }

    /**
     * 加载所有省市
     * @return Result
     */
    public function getProvinces(): Result {
        if (!$datas = Redis::pool()->get('_PROVINCES_DATAS_' . Date::today())) {
            $result = $this->districtQuery(subdistrict: 2);
            if ($result->hasError()) {
                return Result::error($result->getMessage());
            }
            $district = $result->getData('districts')[0]['districts'];
            $datas = [];
            foreach ($district as $item) {
                $citys = [];
                if (!in_array($item['name'], ['北京市', '天津市', '上海市', '重庆市'])) {
                    foreach ($item['districts'] as $city) {
                        $citys[] = [
                            'text' => $city['name'],
                            'value' => $city['adcode'],
                            'pinyin' => Pinyin::abbr(mb_substr($city['name'], 0, 1))[0]
                        ];
                    }
                } else {
                    $subResult = $this->districtQuery($item['name'], subdistrict: 2);
                    if ($subResult->hasError()) {
                        return Result::error($subResult->getMessage());
                    }
                    $subDistrict = $subResult->getData('districts')[0]['districts'];
                    foreach ($subDistrict as $subItem) {
                        foreach ($subItem['districts'] as $subCity) {
                            $citys[] = [
                                'text' => $subCity['name'],
                                'value' => $subCity['adcode'],
                                'pinyin' => Pinyin::abbr(mb_substr($subCity['name'], 0, 1))[0]
                            ];
                        }
                    }
                }
                usort($citys, [self::class, 'compareByTextInitial']);
                $datas[] = [
                    'text' => $item['name'],
                    'value' => $item['adcode'],
                    'pinyin' => Pinyin::abbr(mb_substr($item['name'], 0, 1))[0],
                    'children' => $citys
                ];
            }
            usort($datas, [self::class, 'compareByTextInitial']);
            Redis::pool()->set('_PROVINCES_DATAS_' . Date::today(), $datas, Date::today('timestamp')['end'] - time());
        }
        return Result::success($datas);
    }

    /**
     * 区域查询
     * @param string $keyword
     * @param int $subdistrict
     * @param string $extensions
     * @return Result
     */
    public function districtQuery(string $keyword = "", int $subdistrict = 3, string $extensions = 'base'): Result {
        $url = $this->districtGateway . '?key=' . $this->ak . '&keywords=' . $keyword . '&subdistrict=' . $subdistrict . '&extensions=' . $extensions;
        return $this->get($url);
    }

    /**
     * @param $url
     * @return Result
     */
    protected function get($url): Result {
        $client = Http::create($url);
        $result = $client->get(60);
        if ($result->hasError()) {
            return Result::error($result->getMessage());
        }
        try {
            return Result::success($result->getData());
        } catch (\Exception $e) {
            return Result::error($e->getMessage());
        }
    }
}