<?php
/**
 * Created by PhpStorm.
 * User: lanhai-pc
 * Date: 2020/7/18
 * Time: 12:01
 */

namespace Scf\Component;

use Endroid\QrCode\Color\Color;
use Endroid\QrCode\Encoding\Encoding;
use Endroid\QrCode\QrCode as Creater;
use Endroid\QrCode\Writer\PngWriter;
use Exception;
use Scf\Cloud\Ali\Oss;
use Scf\Core\Component;
use Scf\Core\Result;
use Scf\Mode\Web\Log;
use Scf\Util\RGB;

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
        $this->creater->setForegroundColor(new Color(0, 0, 0));
        $this->creater->setBackgroundColor(new Color(255, 255, 255));

        $this->creater->setEncoding(new Encoding('UTF-8'));
        $this->creater->setMargin(10);
        return $this;
    }

    /**
     * 上传到服务器
     * @param int $size
     * @param string $return
     * @return Result
     */
    public function upload(int $size = 300, string $return = "url"): Result {
        $data = $this->base64($size);
        if (empty($data)) {
            return Result::error('二维码上传失败,文件内容为空');
        }
        return Oss::instance()->upload(base64_decode(str_replace("data:image/png;base64,", "", $data), true), 'png', return: $return);
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