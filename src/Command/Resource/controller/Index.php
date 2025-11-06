<?php

namespace App\Demo\Controller;

use Scf\Core\Result;
use Scf\Mode\Web\Controller;
use Scf\Mode\Web\Request;

/**
 * @module('Demo')
 * @moduleName('演示模块')
 * @controllerName('首页控制器')
 */
class Index extends Controller {
    /**
     * 演示1
     * @Route("/demo/{param1}/")
     * @param int $param1 参数1
     * @get id string URI参数;R;max:1000;min:1;default:1
     * @post body string POST参数;R;max:1000;min:1
     * @result data.name string 列表数据字段1
     * @result data.value string 列表数据字段2
     * @result data.path string 路径参数
     * @result data.p1 int 返回数据1
     * @result data.p2 int 返回数据2
     * @result data.p3.p31 string 嵌套返回数据1
     * @result data.p3.p32 string 嵌套返回数据2
     * @result data.list array 列表数据
     * @result data.list.name string 列表数据字段1
     * @result data.list.value string 列表数据字段2
     * @return Result
     */
    public function index(int $param1): Result {
        Request::post([
            'body'
        ])->pack($body);
        Request::get([
            'id'
        ])->pack($get);
        return Result::success([
            'path' => $param1,
            'body' => $body,
            'get' => $get,
            'data' => [
                'list' => [
                    ['name' => 'name', 'value' => 'value'],
                    ['name' => 'name2', 'value' => 'value2']
                ],
                'p1' => 1,
                'p2' => 2,
                'p3' => [
                    'p31' => '31',
                    'p32' => '32'
                ]
            ]
        ]);
    }

    /**
     * 演示2
     * @Route('/demo/root/')
     * @result data array 列表数据
     * @result data.name string 列表数据字段1
     * @result data.value string 列表数据字段2
     * @return Result
     */
    public function root(): Result {

        return Result::success([
            ['name' => 'name', 'value' => 'value'],
            ['name' => 'name2', 'value' => 'value2']
        ]);
    }

    /**
     * 演示3
     * @Route('/demo/list/')
     * @result data.list array 列表数据
     * @result data.list.name string 列表数据字段1
     * @result data.list.value string 列表数据字段2
     * @return Result
     */
    public function list(): Result {

        return Result::success([
            'list' => [
                ['name' => 'name', 'value' => 'value'],
                ['name' => 'name2', 'value' => 'value2']
            ],
            'total' => 2
        ]);
    }
}