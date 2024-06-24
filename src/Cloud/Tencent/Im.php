<?php

namespace Scf\Cloud\Tencent;

use Hedeqiang\TenIM\Exceptions\Exception;
use Hedeqiang\TenIM\Exceptions\HttpException;
use Hedeqiang\TenIM\IM as TencentIM;
use Scf\Cloud\TencentCloud;
use Scf\Core\Result;
use Scf\Util\Sn;

class Im extends TencentCloud {
    protected TencentIM $client;

    public function _init() {
        parent::_init();
        $this->client = new TencentIM($this->_config);
    }

    /**
     * 导入用户
     * @param $nickname
     * @param $headImage
     * @return Result
     */
    public function importUser($nickname, $headImage): Result {
        $id = Sn::create_uuid();
        $params = [
            'Identifier' => $id,
            'Nick' => $nickname,
            'FaceUrl' => $headImage,
        ];
        $result = $this->send('im_open_login_svc', 'account_import', $params);
        if (!$result->isOk()) {
            return Result::error($result->getErrInfo());
        }
        return Result::success($params);
    }

    /**
     * @param $server
     * @param $command
     * @param $params
     * @return ImApiResult
     */
    protected function send($server, $command, $params): ImApiResult {
        try {
            $result = $this->client->send($server, $command, $params);
            return new ImApiResult($result);
        } catch (HttpException|Exception $exception) {
            return new ImApiResult([
                'ActionStatus' => 'FAIL',
                'ErrorInfo' => $exception->getMessage(),
                'ErrorCode' => $exception->getCode()
            ]);
        }
    }

}

