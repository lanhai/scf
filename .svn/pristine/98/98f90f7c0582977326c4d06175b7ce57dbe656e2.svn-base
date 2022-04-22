<?php

namespace Scf\Mode\Web\Exception;

use Scf\Core\Exception;

class NotFoundException extends Exception {
    protected $message;

    public function __construct($message = "", $code = 404, \Exception $previous = null) {
        $message = $message ?: "Page Not Found";
        \Exception::__construct($message, $code, $previous);
    }

}