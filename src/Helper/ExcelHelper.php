<?php

namespace Scf\Helper;

use Scf\Cloud\Ali\Oss;
use Scf\Core\Result;
use Vtiful\Kernel\Excel;

class ExcelHelper {
    /**
     * @param array $data
     * @param array $headerMaps
     * @param ?string $fileName
     * @return Result
     */
    public static function export(array $data, array $headerMaps, ?string $fileName = null): Result {
        $exportDatas = [];
        $header = [];
        $keys = [];
        foreach ($headerMaps as $key => $title) {
            $keys[] = $key;
            $header[] = $title;
        }
        foreach ($data as $item) {
            $row = [];
            foreach ($keys as $key) {
                if ($key == 'created' && isset($item[$key]) && is_numeric($item[$key])) {
                    $item[$key] = date('Y-m-d H:i:s', $item[$key]);
                }
                $row[] = $item[$key] ?? "--";
            }
            $exportDatas[] = $row;
        }
        $config = [
            'path' => APP_PATH . '/tmp' // xlsx文件保存路径
        ];
        $excel = new Excel($config);
        //fileName 会自动创建一个工作表，你可以自定义该工作表名称，工作表名称为可选参数
        $filePath = $excel->fileName($fileName, 'sheet1')
            ->header($header)
            ->data($exportDatas)
            ->output();
        //var_dump(basename($filePath));
        //$extension = pathinfo($filePath, PATHINFO_EXTENSION);
        //$uploadResult = Oss::instance()->upload(file_get_contents($filePath), $extension);
        $uploadResult = Oss::instance()->uploadFile($filePath, '/upload/excel/' . date('Ymd') . '/' . $fileName);
        if ($uploadResult->hasError()) {
            return Result::error($uploadResult->getMessage());
        }
        @unlink($filePath);
        return Result::success($uploadResult->getData());
    }
}