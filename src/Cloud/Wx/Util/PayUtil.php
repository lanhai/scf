<?php

namespace Scf\Cloud\Wx\Util;

use App\Wx\Util\src;
use App\Wx\Util\token;
use App\Wx\Util\type;
use App\Wx\Util\value;
use function App\Wx\Util\auth_encode;
use function App\Wx\Util\get_url_content;

/**
 * 微信支付通用工具类
 */
class PayUtil {

    /**
     * 创建支付支付js代码
     * @param string $OrderPayUrl 应用支付模板地址
     * @param string $data 订单数据
     * @param string $returnUrl 支付成功后返回的URL
     * @param $jsapiTicket
     * @param $payId
     * @return string|array
     */
    public static function build_pay_code(string $OrderPayUrl, string $data, string $returnUrl, $jsapiTicket, $payId): string|array {
        $order = @auth_encode(serialize($data));
        $jsapiTicket = json_encode($jsapiTicket);
        $payHtml = <<<EOT
	    <script src="http://static.wx.weiwubao.com/asset/common/js/wxapi.js?v=201702221556"></script>
        <script>
			var wxapi = new wxapi();
			wxapi.mchid = {$data['mch_id']};
			wxapi.host='http://www.linktovip.com';
			wxapi.config({$jsapiTicket});
			wxapi.hideTopTool();
        </script>
        <script language="javascript">
			var payButton = $('#wxpay-button');
			var _isPaying = false;
			var payStatus=payStatus || {};
			$(function () {
				 payButton.click(function () {
					pay();
				 });
			});
			var payingDot=1;
			var payingDotStr='';
			var payingTimer;
			function startPaying(){
			   clearTimeout(payingTimer);
			   switch(payingDot){
			       case 1:
			       payingDotStr='.';
			       break;
			       case 2:
			       payingDotStr='..';
			       break;
			       case 3:
			       payingDotStr='...';
			       break;
			   }
			   if(payingDot==3){
			       payingDot=1;
			   }else{
			       payingDot=payingDot+1;
			   }
			   payButton.html('支付中'+payingDotStr);
			   payingTimer=setTimeout(function(){
			      startPaying();
			   },500)
			}
			function stopPaying(){
			   _isPaying = false;
			   clearTimeout(payingTimer);
			   payButton.html('微信支付');
			}
			function checkPay() {
			  		$.ajax({
					url: '/pay/wechat/check/',
					dataType: 'json',
					method: 'get',
					data: {
						id: '{$payId}'
					},
					success: function (json) {
					    if(json.errCode){
					        alert(json.message);
					    }else{
					        if(json.data==1){
					            window.location.href = '{$returnUrl}';
					        }else{
					            setTimeout(function() {
					              checkPay();
					            },1000);
					        }
					    }
					},
					error: function () {
						alert('网络故障,请刷新页面重试');
					},
					complete: function () {
					    
					}
				});
			}
			function pay() {
			    if(_isPaying){
			       return;
			    }
//			    if(typeof window.WeixinJSBridge == 'undefined' || typeof window.WeixinJSBridge.invoke == 'undefined'){
//					$.alert('请在微信内打开此页面完成支付');
//			        return;
//				}
			    _isPaying = true;
			    payingDot=1;
				startPaying();
				$.ajax({
					url: '/pay/wechat/jspay/',
					dataType: 'json',
					method: 'post',
					data: {
						key: "{!$order!}"
					},
					success: function (json) {
						if (json.status) {
						    if(json.data.trade_type=='MWEB'){
						         stopPaying();
						         window.location.href = json.data.mweb_url;
						    }else{
                                 wx.chooseWXPay({
                                    timestamp: json.data.timeStamp, // 支付签名时间戳，注意微信jssdk中的所有使用timestamp字段均为小写。但最新版的支付后台生成签名使用的timeStamp字段名需大写其中的S字符
                                    nonceStr: json.data.nonceStr, //支付签名随机串，不长于 32 位
                                    package: json.data.package, //统一支付接口返回的prepay_id参数值，提交格式如：prepay_id=***）
                                    signType: json.data.signType,//签名方式，默认为'SHA1'，使用新版支付需传入'MD5'
                                    paySign: json.data.paySign, //支付签名
                                    success: function (res) {
                                        stopPaying();
                                        var strs = res.errMsg.split(":"); //字符分割
                                        var status = strs[1];
                                        switch (status) {
                                            case 'ok':
                                                if (typeof(onPaySuccess) != "undefined") {
                                                     onPaySuccess();
                                                }
                                                wxapi.showTopTool();
                                                if (typeof(payCallback) != "undefined") {
                                                     payCallback();
                                                }else{
                                                     _isPaying = true;
                                                     payButton.html('支付成功');
                                                     checkPay();
                                                }
                                                break;
                                            default:
                                                alert('支付失败,错误信息:' + res.errMsg);
                                                break;
                                        }
                                    },
                                    cancel:function(res){
                                        stopPaying();
                                        if (typeof(onPayError) != "undefined") {
                                            onPayError();
                                        }
                                       //$.alert('支付取消');
                                    },
                                    fail:function(res){
                                        stopPaying();
                                        alert('支付失败:'+res.errMsg);
                                    } 
                                });
						    }
						
						} else {
							stopPaying();
							alert(json.msg);
						}
					},
					error: function () {
					    stopPaying();
						alert('网络故障,请刷新页面重试');
					},
					complete: function () {
						return true;
					}
				});
			}
		</script>
EOT;
        $orderHtml = get_url_content($OrderPayUrl);
        if (!$orderHtml) {
            echo '访问:' . $OrderPayUrl . '失败';
            exit;
        }
        $pageHtml = str_replace("</body>", $payHtml . "\n</body>", $orderHtml);
        return $pageHtml;
    }

    /**
     * 创建支付支付js代码
     * @param string $OrderPayUrl 应用支付模板地址
     * @param string $data 订单数据
     * @param string $returnUrl 支付成功后返回的URL
     * @return string
     */
    public static function _build_pay_code($OrderPayUrl, $data, $returnUrl) {
        $order = @auth_encode(serialize($data));
        $payHtml = <<<EOT
        <script src="http://static.weiwubao.com/asset/lib/js/ibox.js"></script>
		<script language="javascript">
			$(function () {
				$('#j-pay-button').click(function () {
					pay();
				});
			});
			function pay() {
				$.iBox.loading('请稍后');
				$.ajax({
					url: '/pay/wechat/jspay/',
					dataType: 'json',
					method: 'post',
					data: {
						key: '{$order}'
					},
					success: function (json) {
						$.iBox.close();
						if (json.status) {
							WeixinJSBridge.invoke('getBrandWCPayRequest', JSON.parse(json.data.package), function (res) {
								var strs = res.err_msg.split(":"); //字符分割
								var status = strs[1];
								switch (status) {
									/*****支付取消*****/
									case 'cancel':
										$.iBox.alert('您取消了支付');
										break;
										/*****支付失败*****/
									case 'fail':
										alert('支付失败,错误信息:' + res.err_msg);
										break;
										/*****支付成功*****/
									case 'ok':
										$.iBox.success('支付成功');
										window.location.href = '{$returnUrl}';
										break;
									default:
										alert('支付失败,错误信息:' + res.err_msg);
										//$.iBox.alert('支付失败,错误信息:' + res.err_msg);
										break;
								}
							});
						} else {
							alert(json.msg);
						}
					},
					error: function () {
						$.iBox.error('系统错误,请稍后再试');
					},
					complete: function () {
						return true;
					}
				});
			}
		</script>
EOT;
        $orderHtml = get_url_content($OrderPayUrl);
        $pageHtml = str_replace("</body>", $payHtml . "\n</body>", $orderHtml);
        return $pageHtml;
    }

    /**
     * 创建订单数据加密串
     * @param type $data
     * @return string
     */
    public static function create_pay_order_data($data): string {
        return @auth_encode(serialize($data));
    }

    /**
     * 创建一个外部订单号
     * @param string $app
     * @return string
     */
    public static function create_out_trade_num($app): string {
        return date('YmdHis') . rand(10000, 99999);//strtoupper($app) .
    }

    /**
     * 给一个URL新增参数
     * @param type $toURL
     * @param type $paras
     * @return string
     */
    public static function genAllUrl(type $toURL, type $paras): string {
        if (strripos($toURL, "?") == "") {
            $allUrl = $toURL . "?" . $paras;
        } else {
            $allUrl = $toURL . "&" . $paras;
        }
        return $allUrl;
    }

    /**
     * 生成一个随机字符串
     * @param int $length 字符串长度
     * @return string
     */
    public static function create_noncestr(int $length = 16): string {
        $chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        $str = "";
        for ($i = 0; $i < $length; $i++) {
            $str .= substr($chars, mt_rand(0, strlen($chars) - 1), 1);
            //$str .= $chars[ mt_rand(0, strlen($chars) - 1) ];
        }
        return $str;
    }

    /**
     * @param src
     * @param token
     * @return
     */
    public static function splitParaStr($src, $token) {
        $resMap = array();
        $items = explode($token, $src);
        foreach ($items as $item) {
            $paraAndValue = explode("=", $item);
            if ($paraAndValue != "") {
                $resMap[$paraAndValue[0]] = $paraAndValue[1];
            }
        }
        return $resMap;
    }

    /**
     * trim
     * @param value
     * @return mixed|null
     */
    public static function trimString($value): mixed {
        $ret = null;
        if (null != $value) {
            $ret = $value;
            if (strlen($ret) == 0) {
                $ret = null;
            }
        }
        return $ret;
    }

    /**
     * 将签名参数数组转换为签名字符串
     * @param array $paraMap 签名数组
     * @param boolean $urlencode 是否进行urlencode 转码
     * @return string
     */
    public static function formatQueryParaMap(array $paraMap, bool $urlencode): string {
        $buff = "";
        ksort($paraMap);
        foreach ($paraMap as $k => $v) {
            if (null != $v && "null" != $v && "sign" != $k) {
                if ($urlencode) {
                    $v = urlencode($v);
                }
                $buff .= $k . "=" . $v . "&";
            }
        }
        $reqPar = '';
        if (strlen($buff) > 0) {
            $reqPar = substr($buff, 0, strlen($buff) - 1);
        }
        return $reqPar;
    }

    /**
     * 将签名参数数组转换为签名字符串(旧接口)
     * @param array $paraMap 签名数组
     * @param boolean $urlencode 是否进行urlencode 转码
     * @return string
     */
    public static function formatBizQueryParaMap(array $paraMap, bool $urlencode): string {
        $buff = "";
        ksort($paraMap);
        foreach ($paraMap as $k => $v) {
            if ($urlencode) {
                $v = urlencode($v);
            }
            $buff .= strtolower($k) . "=" . $v . "&";
        }
        $reqPar = '';
        if (strlen($buff) > 0) {
            $reqPar = substr($buff, 0, strlen($buff) - 1);
        }
        return $reqPar;
    }

    public static function verifySignature($content, $sign, $md5Key): bool {
        $signStr = $content . "&key=" . $md5Key;
        $calculateSign = strtolower(md5($signStr));
        $tenpaySign = strtolower($sign);
        return $calculateSign == $tenpaySign;
    }

}
