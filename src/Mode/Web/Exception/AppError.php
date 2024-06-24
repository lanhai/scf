<?php

namespace Scf\Mode\Web\Exception;

use Scf\Core\Exception;

class AppError extends Exception {
    protected $message;

    public function __construct($message = "", $code = 500, \Exception $previous = null) {
        \Exception::__construct($message, $code, $previous);
    }

}