<?php
namespace Jepsonwu\database\cache;
/**
 * Created by PhpStorm.
 * User: jepsonwu
 * Date: 2017/2/7
 * Time: 10:45
 */
interface Cache
{
    public function get($key);

    public function getMulti(array $keys);

    public function set($key, $value, $expiration);

    public function setMulti(array $items, $expiration);

    public function delete($key);

    public function deleteMulti(array $keys);

    public function flush();
}