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
namespace Jcode\Cache\Redis;

if (!defined('CRLF')) {
    define('CRLF', sprintf('%s%s', chr(13), chr(10)));
}

class Client
{

    const TYPE_STRING = 'string';

    const TYPE_LIST = 'list';

    const TYPE_SET = 'set';

    const TYPE_ZSET = 'zset';

    const TYPE_HASH = 'hash';

    const TYPE_NONE = 'none';

    const FREAD_BLOCK_SIZE = 8192;

    /**
     * Socket connection to the Redis server or Redis library instance
     * @var resource|Redis
     */
    protected $redis;
    protected $redisMulti;

    /**
     * Host of the Redis server
     * @var string
     */
    protected $host;

    /**
     * Port on which the Redis server is running
     * @var integer
     */
    protected $port;

    /**
     * Timeout for connecting to Redis server
     * @var float
     */
    protected $timeout;

    /**
     * Timeout for reading response from Redis server
     * @var float
     */
    protected $readTimeout;

    /**
     * Unique identifier for persistent connections
     * @var string
     */
    protected $persistent;

    /**
     * @var bool
     */
    protected $closeOnDestruct = true;

    /**
     * @var bool
     */
    protected $connected = false;

    /**
     * @var bool
     */
    protected $standalone;

    /**
     * @var int
     */
    protected $maxConnectRetries = 0;

    /**
     * @var int
     */
    protected $connectFailures = 0;

    /**
     * @var bool
     */
    protected $usePipeline = false;

    /**
     * @var array
     */
    protected $commandNames;

    /**
     * @var string
     */
    protected $commands;

    /**
     * @var bool
     */
    protected $isMulti = false;

    /**
     * @var bool
     */
    protected $isWatching = false;

    /**
     * @var string
     */
    protected $authPassword;

    /**
     * @var int
     */
    protected $selectedDb = 0;

    /**
     * Aliases for backwards compatibility with phpredis
     * @var array
     */
    protected $aliasedMethods = [
        'delete' => 'del',
        'getkeys' => 'keys',
        'sremove' => 'srem'
    ];

    /**
     * Creates a Redisent connection to the Redis server on host {@link $host} and port {@link $port}.
     * $host may also be a path to a unix socket or a string in the form of tcp://[hostname]:[port] or unix://[path]
     *
     * @param string $host The hostname of the Redis server
     * @param integer $port The port number of the Redis server
     * @param float $timeout Timeout period in seconds
     * @param string $persistent Flag to establish persistent connection
     */
    public function __construct($host = '127.0.0.1', $port = 6379, $timeout = null, $persistent = '')
    {
        $this->host = (string)$host;
        $this->port = (int)$port;
        $this->timeout = $timeout;
        $this->persistent = (string)$persistent;
        $this->standalone = !extension_loaded('redis');
    }

    public function __destruct()
    {
        if ($this->closeOnDestruct) {
            $this->close();
        }
    }

    /**
     * @return $this
     * @throws \Jcode\Cache\Redis\Exception
     */
    public function forceStandalone()
    {
        if ($this->connected) {
            throw new Exception("Cannot force Redis client to use standalone PHP driver after a connection already has been established");
        }

        $this->standalone = true;

        return $this;
    }

    /**
     * @param $num
     *
     * @return $this
     */
    public function setMaxConnectRetries($num)
    {
        $this->maxConnectRetries = $num;

        return $this;
    }

    public function connect()
    {
        if ($this->connected) {
            return $this;
        }

        if (preg_match('#^(tcp|unix)://(.*)$#', $this->host, $matches)) {
            if ($matches[1] == 'tcp') {
                if (!preg_match('#^(.*)(?::(\d+))?(?:/(.*))?$#', $matches[2], $matches)) {
                    throw new Exception('Invalid host format; expected tcp://host[:port][/persistent]');
                }
                $this->host = $matches[1];
                $this->port = (int)(isset($matches[2]) ? $matches[2] : 6379);
                $this->persistent = isset($matches[3]) ? $matches[3] : '';
            } else {
                $this->host = $matches[2];
                $this->port = null;
                if (substr($this->host, 0, 1) != '/') {
                    throw new Exception('Invalid unix socket format; expected unix:///path/to/redis.sock');
                }
            }
        }

        if ($this->port !== null && substr($this->host, 0, 1) == '/') {
            $this->port = null;
        }

        if ($this->standalone) {
            $flags = STREAM_CLIENT_CONNECT;
            $remote_socket = $this->port === null ? 'unix://' . $this->host : 'tcp://' . $this->host . ':' . $this->port;

            if ($this->persistent) {
                if ($this->port === null) {
                    throw new Exception('Persistent connections to UNIX sockets are not supported in standalone mode.');
                }

                $remote_socket .= '/' . $this->persistent;
                $flags = $flags | STREAM_CLIENT_PERSISTENT;
            }

            $result = $this->redis = @stream_socket_client(
                $remote_socket,
                $errno,
                $errstr,
                $this->timeout !== null ? $this->timeout : 2.5,
                $flags
            );
        } else {
            if (!$this->redis) {
                $this->redis = new \Redis;
            }

            $result = $this->persistent
                ? $this->redis->pconnect($this->host, $this->port, $this->timeout, $this->persistent)
                : $this->redis->connect($this->host, $this->port, $this->timeout);
        }

        // Use recursion for connection retries
        if (!$result) {
            $this->connectFailures++;

            if ($this->connectFailures <= $this->maxConnectRetries) {
                return $this->connect();
            }

            $failures = $this->connectFailures;
            $this->connectFailures = 0;

            throw new Exception("Connection to Redis failed after $failures failures.");
        }

        $this->connectFailures = 0;
        $this->connected = false;

        // Set read timeout
        if ($this->readTimeout) {
            $this->setReadTimeout($this->readTimeout);
        }

        return $this;
    }

    /**
     * Set the read timeout for the connection. If falsey, a timeout will not be set. Negative values not supported.
     *
     * @param $timeout
     * @throws Exception
     * @return Credis_Client
     */
    public function setReadTimeout($timeout)
    {
        if ($timeout < 0) {
            throw new Exception('Negative read timeout values are not supported.');
        }

        $this->readTimeout = $timeout;

        if ($this->connected) {
            if ($this->standalone) {
                stream_set_timeout($this->redis, (int)floor($timeout), ($timeout - floor($timeout)) * 1000000);
            } else {
                if (defined('Redis::OPT_READ_TIMEOUT')) {
                    $this->redis->setOption(\Redis::OPT_READ_TIMEOUT, $timeout);
                }
            }
        }

        return $this;
    }

    /**
     * @return bool
     */
    public function close()
    {
        $result = true;

        if ($this->connected && !$this->persistent) {
            try {
                $result = $this->standalone ? fclose($this->redis) : $this->redis->close();
                $this->connected = false;
            } catch (Exception $e) {
            }
        }

        return $result;
    }

    /**
     * @param string $password
     * @return bool
     */
    public function auth($password)
    {
        $this->authPassword = $password;
        $response = $this->__call('auth', [$this->authPassword]);

        return $response;
    }

    /**
     * @param int $index
     * @return bool
     */
    public function select($index)
    {
        $this->selectedDb = (int)$index;
        $response = $this->__call('select', [$this->selectedDb]);

        return $response;
    }

    public function __call($name, $args)
    {
        $this->connect();

        $name = strtolower($name);

        if ($this->standalone) {
            $argsFlat = null;

            foreach ($args as $index => $arg) {
                if (is_array($arg)) {
                    if ($argsFlat === null) {
                        $argsFlat = array_slice($args, 0, $index);
                    }

                    if ($name == 'mset' || $name == 'msetnx' || $name == 'hmset') {
                        foreach ($arg as $key => $value) {
                            $argsFlat[] = $key;
                            $argsFlat[] = $value;
                        }
                    } else {
                        $argsFlat = array_merge($argsFlat, $arg);
                    }
                } else {
                    if ($argsFlat !== null) {
                        $argsFlat[] = $arg;
                    }
                }
            }
            if ($argsFlat !== null) {
                $args = $argsFlat;
                $argsFlat = null;
            }

            if ($this->usePipeline) {
                if ($name == 'pipeline') {
                    throw new Exception('A pipeline is already in use and only one pipeline is supported.');
                } else {
                    if ($name == 'exec') {
                        if ($this->isMulti) {
                            $this->commandNames[] = $name;
                            $this->commands .= self::prepareCommand([$name]);
                        }

                        if ($this->commands) {
                            $this->writeCommand($this->commands);
                        }

                        $this->commands = null;

                        $response = [];

                        foreach ($this->commandNames as $command) {
                            $response[] = $this->readReply($command);
                        }

                        $this->commandNames = null;

                        if ($this->isMulti) {
                            $response = array_pop($response);
                        }

                        $this->usePipeline = $this->isMulti = false;

                        return $response;
                    } else {
                        if ($name == 'multi') {
                            $this->isMulti = true;
                        }

                        array_unshift($args, $name);

                        $this->commandNames[] = $name;
                        $this->commands .= self::prepareCommand($args);

                        return $this;
                    }
                }
            }

            if ($name == 'pipeline') {
                $this->usePipeline = true;
                $this->commandNames = [];
                $this->commands = '';

                return $this;
            }

            if ($name == 'unwatch') {
                $this->isWatching = false;
            }

            array_unshift($args, $name);

            $command = self::prepareCommand($args);

            $this->writeCommand($command);

            $response = $this->readReply($name);

            if ($name == 'watch') {
                $this->isWatching = true;
            } else {
                if ($this->isMulti && ($name == 'exec' || $name == 'discard')) {
                    $this->isMulti = false;
                } else {
                    if ($this->isMulti || $name == 'multi') {
                        $this->isMulti = true;
                        $response = $this;
                    }
                }
            }
        } else {
            switch ($name) {
                case 'get':   // optimize common cases
                case 'set':
                case 'hget':
                case 'hset':
                case 'setex':
                case 'mset':
                case 'msetnx':
                case 'hmset':
                case 'hmget':
                case 'del':
                    break;
                case 'mget':
                    if (isset($args[0]) && !is_array($args[0])) {
                        $args = [$args];
                    }

                    break;
                case 'lrem':
                    $args = [$args[0], $args[2], $args[1]];

                    break;
                default:
                    $argsFlat = null;

                    foreach ($args as $index => $arg) {
                        if (is_array($arg)) {
                            if ($argsFlat === null) {
                                $argsFlat = array_slice($args, 0, $index);
                            }

                            $argsFlat = array_merge($argsFlat, $arg);
                        } else {
                            if ($argsFlat !== null) {
                                $argsFlat[] = $arg;
                            }
                        }
                    }

                    if ($argsFlat !== null) {
                        $args = $argsFlat;
                        $argsFlat = null;
                    }
            }

            try {
                if ($name == 'pipeline' || $name == 'multi') {
                    if ($this->isMulti) {
                        return $this;
                    } else {
                        $this->isMulti = true;
                        $this->redisMulti = call_user_func_array([$this->redis, $name], $args);
                    }
                } else {
                    if ($name == 'exec' || $name == 'discard') {
                        $this->isMulti = false;
                        $response = $this->redisMulti->$name();
                        $this->redisMulti = null;

                        return $response;
                    }
                }

                if (isset($this->aliasedMethods[$name])) {
                    $name = $this->aliasedMethods[$name];
                }

                if ($this->isMulti) {
                    call_user_func_array([$this->redisMulti, $name], $args);

                    return $this;
                }

                $response = call_user_func_array([$this->redis, $name], $args);
            } catch (Exception $e) {
                throw new Exception($e->getMessage(), $e->getCode());
            }

            switch ($name) {
                case 'hmget':
                    $response = array_values($response);

                    break;
                case 'type':
                    $typeMap = array(
                        self::TYPE_NONE,
                        self::TYPE_STRING,
                        self::TYPE_SET,
                        self::TYPE_LIST,
                        self::TYPE_ZSET,
                        self::TYPE_HASH,
                    );

                    $response = $typeMap[$response];

                    break;
            }
        }

        return $response;
    }

    protected function writeCommand($command)
    {
        if (feof($this->redis)) {
            $this->close();

            if (($this->isMulti && !$this->usePipeline) || $this->isWatching) {
                $this->isMulti = $this->isWatching = false;

                throw new Exception('Lost connection to Redis server during watch or transaction.');
            }

            $this->connected = false;
            $this->connect();

            if ($this->authPassword) {
                $this->auth($this->authPassword);
            }

            if ($this->selectedDb != 0) {
                $this->select($this->selectedDb);
            }
        }

        $commandLen = strlen($command);

        for ($written = 0; $written < $commandLen; $written += $fwrite) {
            $fwrite = fwrite($this->redis, substr($command, $written));

            if ($fwrite === false || $fwrite == 0) {
                throw new Exception('Failed to write entire command to stream');
            }
        }
    }

    protected function readReply($name = '')
    {
        $reply = fgets($this->redis);

        if ($reply === false) {
            throw new Exception('Lost connection to Redis server.');
        }

        $reply = rtrim($reply, CRLF);

        $replyType = substr($reply, 0, 1);

        switch ($replyType) {
            case '-':
                if ($this->isMulti || $this->usePipeline) {
                    $response = false;
                } else {
                    throw new Exception(substr($reply, 4));
                }

                break;
            case '+':
                $response = substr($reply, 1);

                if ($response == 'OK' || $response == 'QUEUED') {
                    return true;
                }

                break;
            case '$':
                if ($reply == '$-1') {
                    return false;
                }

                $size = (int)substr($reply, 1);
                $response = stream_get_contents($this->redis, $size + 2);

                if (!$response) {
                    throw new Exception('Error reading reply.');
                }

                $response = substr($response, 0, $size);

                break;
            case '*':
                $count = substr($reply, 1);

                if ($count == '-1') {
                    return false;
                }

                $response = [];

                for ($i = 0; $i < $count; $i++) {
                    $response[] = $this->readReply();
                }

                break;
            case ':':
                $response = intval(substr($reply, 1));

                break;
            default:
                throw new Exception('Invalid response: ' . print_r($reply, true));

                break;
        }

        switch ($name) {
            case '': // Minor optimization for multi-bulk replies
                break;
            case 'config':
            case 'hgetall':
                $keys = $values = [];
                while ($response) {
                    $keys[] = array_shift($response);
                    $values[] = array_shift($response);
                }

                $response = count($keys) ? array_combine($keys, $values) : [];

                break;
            case 'info':
                $lines = explode(CRLF, trim($response, CRLF));

                $response = [];

                foreach ($lines as $line) {
                    if (!$line || substr($line, 0, 1) == '#') {
                        continue;
                    }

                    list($key, $value) = explode(':', $line, 2);

                    $response[$key] = $value;
                }

                break;
            case 'ttl':
                if ($response === -1) {
                    $response = false;
                }

                break;
        }

        return $response;
    }

    /**
     * Build the Redis unified protocol command
     *
     * @param array $args
     * @return string
     */
    private static function prepareCommand($args)
    {
        return sprintf('*%d%s%s%s', count($args), CRLF, implode(array_map(['self', 'map'], $args), CRLF), CRLF);
    }

    private static function map($arg)
    {
        return sprintf('$%d%s%s', strlen($arg), CRLF, $arg);
    }
}