<?php

declare(strict_types=1);

namespace Scf\Server\SocketIO\Enum;

/**
 * Class BaseEnum.
 *
 * @method static mixed getTextByCode(mixed $code)
 * @method static mixed getCodeByText(mixed $message)
 * @method static mixed getAllText(array $decorators = [])
 */
abstract class BaseEnum
{
    /** @var array */
    private static $comment = [];

    /**
     * @throws \ReflectionException
     *
     * @return bool
     */
    private static function init()
    {
        if (isset(self::$comment[static::class])) {
            return false;
        }

        $refClass = new \ReflectionClass(static::class);

        $refConstants = $refClass->getReflectionConstants();

        /** @var \ReflectionClassConstant $ref */
        foreach ($refConstants as $ref) {
            $doc = $ref->getDocComment();
            preg_match('/@message\([\'|\"](?P<comment>.+)[\'|\"]\)/', $doc, $matches);
            self::$comment[static::class][$ref->getValue()] = $matches['comment'];
        }
    }

    /**
     * @param $code
     *
     * @return mixed
     */
    private static function _getTextByCode($code)
    {
        $comment = self::$comment[static::class];
        if (!isset($comment[$code])) {
            throw new \RuntimeException("The enumeration's code does not exist：{$code}");
        }

        $result = $comment[$code];
        if (preg_match('/^\d+$/', $result)) {
            $result = (int) $result;
        } elseif (preg_match('/^(true|false)$/', $result)) {
            if ($result === 'true') {
                $result = true;
            } elseif ($result === 'false') {
                $result = false;
            }
        }

        return $result;
    }

    /**
     * @param $message
     *
     * @return mixed
     */
    private static function _getCodeByText($message)
    {
        $comment = self::$comment[static::class];
        $revComment = array_flip($comment);

        if (!isset($revComment[$message])) {
            throw new \RuntimeException("The enumeration's message does not exist：{$message}");
        }

        return $revComment[$message];
    }

    /**
     * @param array $decorators
     *
     * @return array
     */
    private static function _getAllText(array $decorators = []): array
    {
        $comment = self::$comment[static::class];
        /** @var BaseDecorator $decorator */
        foreach ($decorators as $decorator) {
            if ($decorator instanceof BaseDecorator) {
                $comment = $decorator->handler($comment);
            }
        }

        return $comment;
    }

    /**
     * @param $name
     * @param $arguments
     *
     * @throws \ReflectionException
     *
     * @return mixed
     */
    public static function __callStatic($name, $arguments)
    {
        if ($name === 'init') {
            throw new \RuntimeException("Can't invoke static::init()");
        }

        self::init();

        $method = "_{$name}";
        if (!method_exists(self::class, $method)) {
            throw new \RuntimeException("undefind static::{$name}()");
        }

        return call_user_func([static::class, $method], ...$arguments);
    }
}
