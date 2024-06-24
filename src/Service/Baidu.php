<?php

namespace Scf\Service;

use Scf\Client\Http;
use Scf\Core\Result;
use Scf\Core\Traits\ComponentTrait;
use Scf\Core\Traits\Singleton;
use Scf\Cache\Redis;

class Baidu {

    use ComponentTrait, Singleton;

    protected string $ak = 'Ge7RT6rViHDWwXVdFG9qt3Q4EGxXnRCt';
    /**
     * @var string 逆地址编码
     */
    protected string $gateway_reverse = 'https://api.map.baidu.com/reverse_geocoding/v3/';

    protected string $gateway_geocoding = 'https://api.map.baidu.com/geocoding/v3/';
    /**
     * @var string 行政区划查询
     */
    protected string $gateway_region = 'https://api.map.baidu.com/api_region_search/v1/';
    /**
     * @var string 天气查询
     */
    protected string $gateway_weather = 'https://api.map.baidu.com/weather/v1/';
    /**
     * @var string IP地址查询
     */
    protected string $gateway_ip = 'https://api.map.baidu.com/location/ip';
    /**
     * @var string 周边搜索
     */
    protected string $gateway_around_search = 'https://api.map.baidu.com/place/v2/search';

    /**
     * 查询IP地址地理位置
     * @param $ip
     * @return Result
     */
    public function ipLocation($ip): Result {
        $pool = Redis::pool();
        $url = $this->gateway_ip . '?ak=' . $this->ak . '&ip=' . $ip . '&coor=bd09ll';
        $key = 'BAIDU_IP_QUERY_' . md5($url);
        if (!$places = $pool->get($key)) {
            $result = $this->get($url);
            if ($result->hasError()) {
                return $result;
            }
            $places = $result->getData();
            $pool->set($key, $places, 300);
        }
        return Result::success($places);
    }

    /**
     * 周边区域检索
     * @param array $location
     * @param string $query
     * @param int $radius
     * @param int $size
     * @return Result
     */
    public function around(array $location, string $query = '美食', int $radius = 1000, int $size = 20): Result {
        $_location = $location['lat'] . ',' . $location['lng'];
        $pool = Redis::pool();
        $url = $this->gateway_around_search . '?ak=' . $this->ak . '&output=json&query=' . $query . '&location=' . $_location . '&radius=' . $radius . '&page_size=' . $size;
        $key = 'BAIDU_AROUND_PLACE_' . md5($url);
        if (!$places = $pool->get($key)) {
            $result = $this->get($url);
            if ($result->hasError()) {
                return $result;
            }
            $places = $result->getData();
            $pool->set($key, $places, 300);
        }
        return Result::success($places);
    }

    /**
     * 地理编码
     * @param $address
     * @param $city
     * @return Result
     */
    public function geocoding($address, $city = null): Result {
        $url = $this->gateway_geocoding . '?ak=' . $this->ak . '&output=json&ret_coordtype=gcj02ll&address=' . $address;
        if (!is_null($city)) {
            $url .= '&city=' . $city;
        }
        return $this->get($url);
    }

    /**
     * 逆地址编码
     * @param $lat
     * @param $lng
     * @return Result
     */
    public function geoReverse($lat, $lng): Result {
        $url = $this->gateway_reverse . '?ak=' . $this->ak . '&output=json&coordtype=gcj02&location=' . $lat . ',' . $lng;
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