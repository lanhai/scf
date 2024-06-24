<?php


namespace App\Admin\Controller\{moduleName};

use Scf\Core\Result;
use App\Admin\Controller\Auth;
use App\{arNamespace} as AR;
use Scf\Database\Driver\Mysql\WhereBuilder;
use Scf\Mode\Web\Request;

class {ApiControllerName} extends Auth {

    /**
     * 详情
     * @return Result
     */
    public function actionDetail(): Result  {
        $id = $this->request()->get('id');
        $item = AR::unique($id)->ar();
        $result = $item->toArray();
        return Result::success($result);
    }

    /**
     * 列表
     * @return Result
     */
    public function actionList(): Result  {
        $keyword = $this->request()->get('keyword');
        $page = $this->request()->get('page');
        $size = $this->request()->get('size');
        $where = WhereBuilder::create();
        if (!empty($keyword)) {
            $where->and(['name[%]' => $keyword]);
        }
        $obj = AR::select();
        $rel = $obj->where($where)->page($page, $size)->list();
        if ($rel['list']) {
            foreach ($rel['list'] as &$item) {

            }
        }
        return Result::success($rel);
    }

    /**
     * 保存
     * @return Result
     */
    public function actionSave(): Result  {
        Request::post()->assign($data);
        $item = AR::factory($data);
        if (!$item->id) {
            $item->created = time();
        }
        if (!$item->save()) {
            return Result::error($item->getError());
        }
        return Result::success($item->toArray());
    }
    /**
      * 删除
      * @return Result
      */
    public function actionRemove(): Result  {
        $id = $this->request()->post('id');
        $item = AR::unique($id)->ar();
        if ($item->notExist()) {
            return Result::error('数据不存在');
        }
        if (!$item->delete()) {
            return Result::error('删除失败');
        }
        return Result::success();
    }
}