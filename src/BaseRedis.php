<?php

declare(strict_types=1);
/**
 * 深圳网通动力网络技术有限公司
 * This file is part of szwtdl/db.
 * @link     https://www.szwtdl.cn
 * @document https://wiki.szwtdl.cn
 * @contact  szpengjian@gmail.com
 * @license  https://github.com/szwtdl/db/blob/master/LICENSE
 */
namespace Szwtdl\DB;

class BaseRedis
{
    protected $pool;

    protected $connection;

    protected $multiOnGoing = false;

    public function __construct($config = null, $poolName = 'default')
    {
        $this->pool = Redis::getInstance($config, $poolName);
    }

    public function __call($name, $arguments)
    {
        if (! $this->multiOnGoing) {
            $this->connection = $this->pool->getConnection();
        }

        try {
            $data = $this->connection->{$name}(...$arguments);
        } catch (\RedisException $e) {
            $this->pool->close(null);
            throw $e;
        }

        if ($this->multiOnGoing) {
            return $this;
        }
        $this->pool->close($this->connection);

        return $data;
    }

    public function brPop($keys, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        $data = [];

        try {
            $start = time();
            $data = $this->connection->brPop($keys, $timeout);
        } catch (\RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                $this->pool->close(null);
                throw $e;
            }
        }

        $this->pool->close($this->connection);

        return $data;
    }

    public function blPop($keys, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        $data = [];

        try {
            $start = time();
            $data = $this->connection->blPop($keys, $timeout);
        } catch (\RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                $this->pool->close(null);
                throw $e;
            }
        }

        $this->pool->close($this->connection);

        return $data;
    }

    public function subscribe($channels, $callback)
    {
        $this->connection = $this->pool->getConnection();

        $this->connection->setOption(\Redis::OPT_READ_TIMEOUT, '-1');

        try {
            $data = $this->connection->subscribe($channels, $callback);
        } catch (\RedisException $e) {
            $this->pool->close(null);
            throw $e;
        }

        $this->connection->setOption(\Redis::OPT_READ_TIMEOUT, (string) $this->pool->getConfig()['time_out']);

        $this->pool->close($this->connection);

        return $data;
    }

    public function brpoplpush($srcKey, $dstKey, $timeout)
    {
        $this->connection = $this->pool->getConnection();

        try {
            $start = time();
            $data = $this->connection->brpoplpush($srcKey, $dstKey, $timeout);
        } catch (\RedisException $e) {
            $end = time();
            if ($end - $start < $timeout) {
                $this->pool->close(null);
                throw $e;
            }
            $data = false;
        }

        $this->pool->close($this->connection);

        return $data;
    }

    public function fill()
    {
        $this->pool->fill();
    }

    public function multi($mode = \Redis::MULTI)
    {
        if (! $this->multiOnGoing) {
            $this->connection = $this->pool->getConnection();

            try {
                $this->connection->multi($mode);
            } catch (\RedisException $e) {
                $this->pool->close(null);
                throw $e;
            }

            $this->multiOnGoing = true;
        }

        return $this;
    }

    public function exec()
    {
        if (! $this->multiOnGoing) {
            return;
        }

        try {
            $result = $this->connection->exec();
        } catch (\RedisException $e) {
            $this->multiOnGoing = false;
            $this->pool->close(null);
            throw $e;
        }

        $this->multiOnGoing = false;

        $this->pool->close($this->connection);

        return $result;
    }
}
