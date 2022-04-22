<?php

namespace Scf\Helper;

class XmlHelper {
    /**
     * 将XML转换为数组
     * @param string $xml
     * @return array
     */
    public static function toArray(string $xml): array {
        $arr = simplexml_load_string($xml, 'SimpleXMLElement', LIBXML_NOCDATA);
        return ObjectHelper::toArray($arr);
    }
}