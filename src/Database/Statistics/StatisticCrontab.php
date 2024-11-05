<?php

namespace Scf\Database\Statistics;

use Scf\Server\Task\Crontab;

class StatisticCrontab extends Crontab {

    public function run(): void {
        StatisticModel::instance()->runQueue();
    }
}