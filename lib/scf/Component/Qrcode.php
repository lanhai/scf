<?php
/**
 * Created by PhpStorm.
 * User: lanhai-pc
 * Date: 2020/7/18
 * Time: 12:01
 */

namespace Scf\Component;

use Endroid\QrCode\QrCode as Creater;
use Exception;
use Scf\Aliyun\Oss;
use Scf\Core\Result;
use Scf\Mode\Web\Log;
use Scf\Util\RGB;
use Scf\Core\Component;
use Endroid\QrCode\Encoding\Encoding;
use Endroid\QrCode\Writer\PngWriter;

class Qrcode extends Component {
    protected string $backgroundColor;
    /**
     * @var Creater
     */
    protected Creater $creater;

    /**
     * 初始化
     * @param $text
     * @return $this
     */
    public function init($text): Qrcode {
        $this->creater = Creater::create($text);
//        $this->creater->setForegroundColor(['r' => 0, 'g' => 0, 'b' => 0, 'a' => 0]);
//        $this->creater->setBackgroundColor(['r' => 255, 'g' => 255, 'b' => 255, 'a' => 0]);
        $this->creater->setEncoding(new Encoding('UTF-8'));
        $this->creater->setMargin(10);
        return $this;
    }

    /**
     * 上传到服务器
     * @param int $size
     * @return Result
     */
    public function upload(int $size = 300): Result {
        $data = $this->base64($size);
        if (empty($data)) {
            return Result::error('二维码上传失败,文件内容为空');
        }
        return Oss::instance()->upload(base64_decode(str_replace("data:image/png;base64,", "", $data), true), 'png');
    }

    /**
     * 返回base64图片数据
     * @param int $size
     * @return string
     */
    public function base64(int $size = 300): string {
        $this->creater->setSize($size);
        $writer = new PngWriter();
        try {
            $result = $writer->write($this->creater);
            return $result->getDataUri();
        } catch (Exception $e) {
            Log::instance()->error('二维码生成失败:' . $e->getMessage());
            return "";
        }
    }

    /**
     * 设置颜色
     * @param $color
     * @return $this
     */
    public function setColor($color): Qrcode {
        $rgb = RGB::ToRGB($color);
        $this->creater->setForegroundColor(['r' => $rgb['r'], 'g' => $rgb['g'], 'b' => $rgb['b'], 'a' => 0]);
        return $this;
    }

    /**
     * 设置图片大小
     * @param $color
     * @return $this
     */
    public function setBackgroundColor($color): Qrcode {
        $rgb = RGB::ToRGB($color);
        $this->creater->setBackgroundColor(['r' => $rgb['r'], 'g' => $rgb['g'], 'b' => $rgb['b'], 'a' => 0]);
        return $this;
    }

    /**
     * 设置背景颜色
     * @param $size
     * @return $this
     */
    public function setSize($size): Qrcode {
        $this->creater->setSize($size);
        return $this;
    }


}