<?php

declare(strict_types=1);

namespace Scf\Server\SocketIO\Event;

/**
 * Class EventPool
 *
 * @package Scf\Server\SocketIO\Event
 */
class EventPool
{
    /** @var EventPool */
    private static $instance;

    /** @var array */
    private $pool = [];

    private function __construct()
    {
    }

    public static function getInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    /**
     * @param EventPayload $payload
     *
     * @return bool
     */
    public function push(EventPayload $payload) : bool
    {
        if (!empty($this->pool)) {
            /** @var EventPayload $item */
            foreach ($this->pool as $item) {
                if ($item->getNamespace() == $payload->getNamespace() && $item->getName() == $payload->getName()) {
                    return true;
                }
            }
        }

        array_push($this->pool, $payload);

        return true;
    }

    /**
     * @param string $namespace
     * @param string $eventName
     *
     * @return bool
     */
    public function isExist(string $namespace, string $eventName) : bool
    {
        /** @var EventPayload $item */
        foreach ($this->pool as $item) {
            if ($item->getNamespace() == $namespace && $item->getName() == $eventName) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param string $namespace
     * @param string $eventName
     *
     * @return EventPayload
     */
    public function pop(string $namespace, string $eventName) : EventPayload
    {
        $eventPayload = new EventPayload();
        $index = null;

        /** @var EventPayload $item */
        foreach ($this->pool as $key => $item) {
            if ($item->getNamespace() == $namespace && $item->getName() == $eventName) {
                $index = $key;
                $eventPayload = $item;
                break;
            }
        }

        if (!is_null($index)) {
            unset($this->pool[$index]);
        }

        return $eventPayload;
    }

    /**
     * @param string $namespace
     * @param string $eventName
     *
     * @return EventPayload
     */
    public function get(string $namespace, string $eventName) : EventPayload
    {
        $eventPayload = new EventPayload();

        /** @var EventPayload $item */
        foreach ($this->pool as $item) {
            if ($item->getNamespace() == $namespace && $item->getName() == $eventName) {
                $eventPayload = $item;
                break;
            }
        }

        return $eventPayload;
    }
}