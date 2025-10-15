<?php

namespace Scf\Database\Statistics;

use Scf\Server\Task\Crontab;
use Throwable;

class StatisticCrontab extends Crontab {

    /**
     * @throws Throwable
     */
    public function run(): void {
        StatisticModel::instance()->runQueue();
    }
}