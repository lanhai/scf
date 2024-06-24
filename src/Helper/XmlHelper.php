<?php

namespace Scf\Helper;

use DOMDocument;
use DOMNode;

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

    public static function domNodeToArray(DOMNode $node): array|string {
        $output = [];
        switch ($node->nodeType) {
            case XML_TEXT_NODE:
                $output = trim($node->nodeValue);
                break;
            case XML_ELEMENT_NODE:
                for ($i = 0, $m = $node->childNodes->length; $i < $m; $i++) {
                    $child = $node->childNodes->item($i);
                    $v = self::domNodeToArray($child);
                    if (isset($child->tagName)) {
                        $t = $child->tagName;
                        if (!isset($output[$t])) {
                            $output[$t] = [];
                        }
                        $output[$t][] = $v;
                    } elseif ($v || $v === '0') {
                        $output = (string)$v;
                    }
                }

                if ($node->attributes->length && !is_array($output)) { // Has attributes but isn't an array
                    $output = ['@content' => $output]; // Change output into an array.
                }

                if (is_array($output)) {
                    if ($node->attributes->length) {
                        $a = [];
                        foreach ($node->attributes as $attrName => $attrNode) {
                            $a[$attrName] = (string)$attrNode->value;
                        }
                        $output['@attributes'] = $a;
                    }
                    foreach ($output as $t => $v) {
                        if (is_array($v) && count($v) == 1 && $t != '@attributes') {
                            $output[$t] = $v[0];
                        }
                    }
                }
                break;
        }

        return $output;
    }
}