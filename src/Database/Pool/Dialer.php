<?php

namespace Scf\Database\Pool;

use Mix\ObjectPool\DialerInterface;
use Scf\Database\Driver;

/**
 * Class Dialer
 * @package Mix\Database\Pool
 */
class Dialer implements DialerInterface {

    /**
     * 数据源格式
     * @var string
     */
    protected string $dsn = '';

    /**
     * 数据库用户名
     * @var string
     */
    protected string $username = 'root';

    /**
     * 数据库密码
     * @var string
     */
    protected string $password = '';

    /**
     * 驱动连接选项
     * @var array
     */
    protected array $options = [];

    /**
     * Dialer constructor.
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array $options
     */
    public function __construct(string $dsn, string $username, string $password, array $options = []) {
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->options = $options;
    }

    /**
     * Dial
     * @return Driver
     */
    public function dial(): object {
        return new Driver(
            $this->dsn,
            $this->username,
            $this->password,
            $this->options
        );
    }

}
