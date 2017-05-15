<?php
/**
 *
 * NOTICE OF LICENSE
 *
 * This source file is subject to the General Public License (GPL 3.0)
 * that is bundled with this package in the file LICENSE
 * It is also available through the world-wide-web at this URL:
 * http://opensource.org/licenses/GPL-3.0
 *
 * DISCLAIMER
 *
 * Do not edit or add to this file if you wish to upgrade this module to newer
 * versions in the future.
 *
 * @category    J!Code: Framework
 * @package     J!Code: Framework
 * @author      Jeroen Bleijenberg <jeroen@jcode.nl>
 *
 * @copyright   Copyright (c) 2017 J!Code (http://www.jcode.nl)
 * @license     http://opensource.org/licenses/GPL-3.0 General Public License (GPL 3.0)
 */
namespace Jcode\Cache;

use Jcode\Application;
use \Exception;
use Jcode\Object;

class Redis implements \Jcode\Cache\CacheInterface
{

    /**
     * @inject \Jcode\Cache\Redis\Client
     * @var \Jcode\Cache\Redis\Client
     */
    protected $redis;

    protected $prefix;

    public function connect(Object $config)
    {
        try {
            $this->redis->connect($config->getHost(), $config->getPort());
            $this->redis->select($config->getDatabase());

            $this->prefix = md5('redis:' . json_encode($this->redis) . ':');
        } catch (Redis\Exception $e) {
            Application::logException($e);
        }

        return $this;

    }

    /**
     * Set value in Redis DB
     *
     * @param $key
     * @param $value
     *
     * @return array|bool|int|\Jcode\Cache\Redis\Client|mixed|string
     * @throws \Jcode\Cache\Redis\Exception
     */
    public function set($key, $value)
    {
        $redis = $this->redis;

        return $redis->__call('hset', [
            md5($key),
            $this->prefix . $key,
            json_encode($value),
        ]);
    }

    /**
     * Get value from Redis
     *
     * @param $key
     *
     * @return mixed
     * @throws \Jcode\Cache\Redis\Exception
     */
    public function get($key)
    {
        $redis = $this->redis;

        $value = $redis->__call('hget', [
            md5($key),
            $this->prefix . $key,
        ]);

        return json_decode($value, true);
    }

    public function exists($key)
    {
        $redis = $this->redis;

        return (bool)$redis->__call('hexists', [
            md5($key),
            $this->prefix . $key
        ]);
    }
}