<?php
declare(strict_types=1);

/** Run actual Struct rule parsing and Dao formatting without a database connection. */
define('DBS_SLAVE', 2);
require dirname(__DIR__) . '/vendor/autoload.php';
spl_autoload_register(static function (string $class): void {
    if (str_starts_with($class, 'Scf\\')) {
        $file = dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
        if (is_file($file)) require $file;
    }
});
class FormatFieldTypeProbe extends \Scf\Database\Dao {
    /**
     * @rule string,max:512
     */
    public ?string $text;
    /**
     * @rule int
     */
    public int|\Scf\Database\Tools\Calculator|null $count;
    /**
     * @rule float
     */
    public ?float $amount;
    /**
     * @rule string
     */
    public int|string|null $mixedText;
    /**
     * @rule string
     */
    public $untyped;
    /**
     * @format code
     */
    public ?string $code;
    /**
     * @format json
     */
    public mixed $json;
}
$dao = new FormatFieldTypeProbe();
$checks = 0;
foreach (['00123', '1e3', ' 00123 ', '+12', '1.20', '0', '12345678901234567890', '&amp;<b>"词"</b>\\'] as $value) {
    foreach (['text', 'mixedText', 'untyped'] as $field) {
        if ($dao->format([$field => $value])[$field] !== $value) throw new RuntimeException("Changed text $field");
        $checks++;
    }
}
foreach ([['count', '012', 12], ['amount', '1.25', 1.25], ['text', null, null], ['count', 4, 4], ['amount', 2.5, 2.5], ['code', '&lt;b&gt;', '<b>']] as [$field, $value, $expected]) {
    if ($dao->format([$field => $value])[$field] !== $expected) throw new RuntimeException("Invalid conversion $field");
    $checks++;
}
if ($dao->format(['json' => '{"n":2}'])['json'] !== ['n' => 2]) throw new RuntimeException('JSON format changed');
echo 'PASS ' . ($checks + 1) . " real Dao field-type format checks\n";
