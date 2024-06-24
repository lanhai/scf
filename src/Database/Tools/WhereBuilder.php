<?php

namespace Scf\Database\Tools;

use JetBrains\PhpStorm\ArrayShape;

class WhereBuilder {

    protected array $and = [];
    protected array $or = [];
    public static string $sql = "";
    /**
     * @var array|string[] 运算符
     */
    protected array $operator = [
        '=' => '=',
        '!' => '<>',
        '%' => 'LIKE',
        '!%' => 'NOT LIKE',
        '^' => 'BETWEEN',
        '!^' => 'NOT BETWEEN',
        '<' => '<',
        '>' => '>',
        '>=' => '>=',
        '<=' => '<=',
        '<=>' => '<=>',
        '~' => 'REGEXP',
    ];

    /**
     * 构造器
     * @param array $condition
     */
    public function __construct(array $condition = []) {
        if ($condition) {
            //$this->and = $condition;
            foreach ($condition as $key => $value) {
                $this->and[] = ['key' => $key, 'value' => $value];
            }
        }
    }

    /**
     * @param array $condition
     * @return WhereBuilder
     */
    public static function create(array $condition = []): static {
        $cls = static::class;
        return new $cls($condition);
    }

    /**
     * 构建AND条件
     * @param $condition
     * @return $this
     */
    public function and($condition): static {
        foreach ($condition as $key => $value) {
            // $this->and[$key] = $value;
            $this->and[] = ['key' => $key, 'value' => $value];
        }
        //$this->and = Arr::merge($this->and, $condition);
        return $this;
    }

    /**
     * 构建OR条件
     * @param $condition
     * @return $this
     */
    public function or($condition): static {
        foreach ($condition as $key => $value) {
            //$this->or[$key] = $value;
            $this->or[] = ['key' => $key, 'value' => $value];
        }
        //$this->or = Arr::merge($this->or, $condition);
        return $this;
    }

    /**
     * 构建sql语句和匹配值
     * @return array
     */
    #[ArrayShape(['sql' => "string", 'match' => "array|array[]"])]
    public function build(): array {
        $sql = "";
        $match = [];
        if ($this->and) {
            $and = $this->sql($this->and, "AND");
            $match = [...$and['match']];
            $sql .= $and['sql'];
        }
        if ($this->or) {
            $or = $this->sql($this->or, "OR");
            $match = [...$match, ...$or['match']];
            $sql .= $sql ? " OR " . $or['sql'] : $or['sql'];
        }
        return ['sql' => $sql, 'match' => $match];
    }

    /**
     * @return ?string
     */
    public function getWhereSql(): ?string {
        $sql = "";
        $where = $this->build();
        $whereSql = $where['sql'];
        if (!$whereSql) {
            return null;
        }
        for ($i = 0; $i < count($where['match']); $i++) {
            $in = $where['match'][$i];
            if (!is_array($in)) {
                $in = is_null($in) ? null : (is_numeric($in) || str_contains($in, "AND") ? $in : "'{$in}'");
            } else {
                $in = implode(',', $in);
            }
            $whereSql = preg_replace("/\?/", "$in", $whereSql, 1);
        }
        $sql .= " WHERE {$whereSql}";
        return $sql;
    }

    /**
     * 生成sql语句
     * @param $data
     * @param $word
     * @return array
     */
    #[ArrayShape(['sql' => "string", 'condition' => "array|mixed", 'match' => "array"])]
    protected function sql($data, $word): array {
        $arr = [
            'condition' => [],
            'match' => []
        ];
        foreach ($data as $item) {
            $format = $this->format($item['key'], $item['value']);
            if (in_array(strtoupper($format['field']), ['OR', 'AND'])) {
                $condition = "{$format['operator']}";
            } else {
                if (in_array($format['operator'], ["BETWEEN", "NOT BETWEEN"])) {
                    $condition = "" . $format['field'] . " {$format['operator']} ? AND ?";
                } else {
                    $condition = "" . $format['field'] . " {$format['operator']}";
                }
            }
            //if (!empty($format['value'])) {
            if (is_array($format['value'])) {
                if (in_array($format['operator'], ["IN", "NOT IN"])) {
                    $arr['match'][] = $format['value'];
                    $condition .= " (?)";
                } else {
                    //$arr['match'] = Arr::merge($arr['match'], $format['value']);
                    $arr['match'] = [...$arr['match'], ...$format['value']];// Arr::merge($orArr['match'],
                }
            } else {
                $arr['match'][] = $format['value'] == '' ? null : $format['value'];// is_numeric($format['value']) ? $format['value'] : "'{$format['value']}'";
                $condition .= " ?";
            }
            //}
            $arr['condition'][] = $condition;
        }

        $sql = implode(" " . strtoupper($word) . " ", $arr['condition']);
        return ['sql' => $sql, 'condition' => $arr['condition'], 'match' => $arr['match']];
    }

    /**
     * 格式化
     * @param $key
     * @param $value
     * @return array
     */
    #[ArrayShape(['field' => "mixed", 'operator' => "mixed|string", 'value' => "array|mixed|string"])]
    protected function format($key, $value): array {
        preg_match('/([a-zA-Z0-9_\.]+)(\[(?<operator>\>\=?|\<\=?|\!?|\<\>|\>\<|~|\^|\!\^|%|\!%|REGEXP)\])?/i', $key, $match);
        $operator = $match['operator'] ?? "=";
        if (!isset($this->operator[$operator])) {
            throw new \PDOException('不合法的比较运算符:' . $operator);
        }
        if (($operator === '%' || $operator === '!%') && !is_array($value) && !str_contains($value, '%')) {
            $value = '%' . $value . '%';
        }
        if (in_array($operator, ['&', '*', '^', '!^']) && !is_array($value)) {
            throw new \PDOException($operator . '运算符比较值必须为数组');
        }
        $formattedOperator = $this->operator[$operator];
        $formattedValue = $value;
        $keyIsOperator = false;
        //if (strtoupper($key) == '#OR' || strtoupper($key) == '#AND') {
        if (str_starts_with(strtoupper($key), '#OR') || str_starts_with(strtoupper($key), '#AND')) {
            $keyIsOperator = true;
            if (!is_array($value) || count($value) < 2) {
                throw new \PDOException($key . '运算符比较值必须为具有两个(含)以上元素的数组');
            }
            $orArr = [
                'condition' => [],
                'match' => []
            ];
            foreach ($value as $k => $v) {
                $andOrFormat = $this->format($k, $v);
                //if (strtoupper($k) == '#OR' || strtoupper($k) == '#AND') {
                if (str_starts_with(strtoupper($k), '#OR') || str_starts_with(strtoupper($k), '#AND')) {
                    $condition = "{$andOrFormat['operator']}";
                } else {
                    $condition = $andOrFormat['field'] . " {$andOrFormat['operator']}";
                }
                $andOrFormat['value'] = $andOrFormat['value'] == '' ? null : $andOrFormat['value'];
                //if (!empty($andOrFormat['value'])) {
                if (is_array($andOrFormat['value'])) {
                    if (!in_array($andOrFormat['operator'], ["IN", "NOT IN"])) {
                        $orArr['match'] = [...$orArr['match'], ...$andOrFormat['value']];// Arr::merge($orArr['match'], $andOrFormat['value']);
                    } else {
                        $orArr['match'][] = $andOrFormat['value'];
                        $condition .= " (?)";
                    }
                } else {
                    $orArr['match'][] = $andOrFormat['value'];
                    $condition .= " ?";
                }
                //}
                $orArr['condition'][] = $condition;
            }
            preg_match('/([A-Z0-9_\.]+)(\[(?<operator>\>\=?|\<\=?|\!?|\<\>|\>\<|~|\^|\!\^|%|\!%|REGEXP)\])?/i', $key, $matchedOperator);
            $formattedOperator = "(" . implode(" " . $matchedOperator[1] . " ", $orArr['condition']) . ")";
            $formattedValue = $orArr['match'];
        } else {
            switch ($operator) {
                case '^':
                case '!^':
                    //$formattedValue = "{$value[0]} AND {$value[1]}";
                    break;
                case '!':
                    if (is_array($value)) {
                        $formattedOperator = 'NOT IN';
                        //$formattedValue = "(" . implode(',', $value) . ")";
                    } elseif (is_null($value)) {
                        $formattedOperator = 'IS NOT';
                        //$formattedValue = NULL;
                    }
                    break;
                case '=':
                    if (is_array($value)) {
                        $formattedOperator = 'IN';
                        //$formattedValue = "(" . implode(',', $value) . ")";
                    } elseif (is_null($value)) {
                        $formattedOperator = 'IS';
                        //$formattedValue = NULL;
                    }
                    break;
                default:
                    if (is_array($value)) {
                        throw new \PDOException($operator . '运算符比较值格式错误');
                    }
                    break;
            }
        }
        if ($formattedOperator === '=' && !is_array($formattedValue) && (str_starts_with($formattedValue, '%') || str_ends_with($formattedValue, '%'))) {
            $formattedOperator = 'LIKE';
        }
        return ['field' => $keyIsOperator ? $match[1] : "`{$match[1]}`", 'operator' => $formattedOperator, 'value' => $formattedValue];
    }
}