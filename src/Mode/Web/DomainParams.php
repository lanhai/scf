<?php

/**
 * 域名定位商户
 */

namespace Scf\Mode\Web;


class DomainParams extends Component {

	protected array $_config = [
		'enable' => false,
		'rule' => [], // 规则, 如:['mpid'=>0], 则会把域名中第一段转换成mpid的值
		'merge_to_get' => false, // 是否合并到$_GET中
	];
	protected array $_params = [];

	protected function _init() {
		$host = Request::host();
		$host = explode('.', $host);
		foreach ($this->_config['rule'] as $name => $index) {
			$this->_params[$name] = $host[$index] ?? null;
		}
		// 合并到GET中
		if ($this->_config['merge_to_get']) {
			foreach ($this->_params as $k => $v) {
				$_GET[$k] = $v;
			}
		}
	}

	public function get($name, $default = null) {
		return $this->_params[$name] ?? $default;
	}

}
