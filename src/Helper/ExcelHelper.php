<?php

namespace Scf\Helper;

use Scf\Cloud\Ali\Oss;
use Scf\Core\Result;
use Vtiful\Kernel\Excel;

class ExcelHelper {
    private Excel $excel;
    private string $filePath = '';
    private string $fileName;
    private array $header;

    public function __construct(string $fileName, array $header, string $sheetName = 'Sheet1') {
        $excel = new Excel([
            'path' => APP_PATH . '/tmp',
        ]);
        $this->fileName = $fileName;
        $this->header = $header;
        $this->excel = $excel->fileName($fileName, $sheetName)->header(array_values($header));
    }

    public static function create(string $fileName, array $header, string $sheetName = 'Sheet1'): static {
        return new static($fileName, $header, $sheetName);
    }

    public function addRows($datas): void {
        $buffer = [];
        foreach ($datas as $item) {
            $row = [];
            foreach (array_keys($this->header) as $key) {
                if ($key === 'created' && isset($item[$key]) && is_numeric($item[$key])) {
                    $item[$key] = date('Y-m-d H:i:s', $item[$key]);
                }
                $row[] = $item[$key] ?? '--';
            }
            $buffer[] = $row;
        }
        unset($datas);
        $this->excel = $this->excel->data($buffer);
        $buffer = [];
        gc_collect_cycles();
    }

    public function addSheet(string $sheetName = 'Sheet1', array $header = []): void {
        $this->header = $header;
        $this->excel = $this->excel->addSheet($sheetName)->header(array_values($header));
    }

    public function output(): string {
        $this->filePath = $this->excel->output();
        return $this->filePath;
    }

    public function upload($output = true): Result {
        if (!$this->filePath || !file_exists($this->filePath)) {
            if ($output) {
                $this->output();
                return $this->upload(false);
            }
            return Result::error('表格文件不存在');
        }
        $uploadResult = Oss::instance()->uploadFile($this->filePath, '/upload/excel/' . date('Ymd') . '/' . $this->fileName);
        if ($uploadResult->hasError()) {
            return Result::error($uploadResult->getMessage());
        }
        @unlink($this->filePath);
        return Result::success($uploadResult->getData());
    }


    /**
     * @param array $data
     * @param array $headerMaps
     * @param ?string $fileName
     * @return Result
     */
    public static function export(array $data, array $headerMaps, ?string $fileName = null): Result {
        $header = [];
        $keys = [];

        foreach ($headerMaps as $key => $title) {
            $keys[] = $key;
            $header[] = $title;
        }

        $config = [
            'path' => APP_PATH . '/tmp' // xlsx文件保存路径
        ];

        $excel = new Excel($config);
        $excel = $excel->fileName($fileName, 'sheet1')
            ->header($header);

        $batchSize = 500; // 每批写入行数，可根据内存情况调整
        $buffer = [];
        $count = 0;

        foreach ($data as $item) {
            $row = [];
            foreach ($keys as $key) {
                if ($key === 'created' && isset($item[$key]) && is_numeric($item[$key])) {
                    $item[$key] = date('Y-m-d H:i:s', $item[$key]);
                }
                $row[] = $item[$key] ?? '--';
            }

            $buffer[] = $row;
            $count++;

            if ($count % $batchSize === 0) {
                $excel = $excel->data($buffer); // 分批写入
                $buffer = [];
                gc_collect_cycles();
            }
        }

        // 写入剩余数据
        if (!empty($buffer)) {
            $excel = $excel->data($buffer);
            $buffer = [];
            gc_collect_cycles();
        }

        $filePath = $excel->output();
        unset($excel);
        $uploadResult = Oss::instance()->uploadFile(
            $filePath,
            '/upload/excel/' . date('Ymd') . '/' . $fileName
        );

        if ($uploadResult->hasError()) {
            return Result::error($uploadResult->getMessage());
        }

        @unlink($filePath);
        return Result::success($uploadResult->getData());
    }
}