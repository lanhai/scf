<?php
declare(strict_types=1);

$root = dirname(__DIR__);
require $root . '/vendor/autoload.php';
spl_autoload_register(static function (string $class) use ($root): void {
    if (str_starts_with($class, 'Scf\\')) {
        require $root . '/src/' . str_replace('\\', '/', substr($class, 4)) . '.php';
    }
});
class StructObjectFixture extends Scf\Core\Struct {
    /** @rule object,class:DateTimeInterface|必须是日期对象 */
    public mixed $date = null;
}
$fixture = new StructObjectFixture();
foreach ([[new DateTimeImmutable(), true], [new stdClass(), false], ['date', false]] as [$value, $expected]) {
    if ($fixture->validate(['date' => $value]) !== $expected) {
        throw new RuntimeException('Unexpected result for ' . get_debug_type($value));
    }
}
echo "PASS Struct object rule checks interface and rejects scalars\n";
