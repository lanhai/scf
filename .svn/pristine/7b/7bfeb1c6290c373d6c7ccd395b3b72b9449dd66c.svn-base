<?php
/**
 * 连客云开发框架
 * User: linkcloud
 * Date: 14-7-16
 * Time: 0:50
 */

namespace Scf\Session;


interface ISession {

	/**
	 * 打开Session
	 * @access public
	 * @param string $savePath
	 * @param mixed $sessName
	 * @return bool
	 */
	public function open(string $savePath, mixed $sessName): bool;

	/**
	 * 关闭Session
	 * @access public
	 */
	public function close();

	/**
	 * 读取Session
	 * @access public
	 * @param string $sessID
	 * @return string
	 */
	public function read(string $sessID): string;

	/**
	 * 写入Session
	 * @access public
	 * @param string $sessID
	 * @param String $sessData
	 * @return bool
	 */
	public function write(string $sessID, string $sessData): bool;

	/**
	 * 删除Session
	 * @access public
	 * @param string $sessID
	 * @return bool
	 */
	public function destroy(string $sessID): bool;

	/**
	 * Session 垃圾回收
	 * @access public
	 * @param string $sessMaxLifeTime
	 * @return int
	 */
	public function gc(string $sessMaxLifeTime): int;
} 