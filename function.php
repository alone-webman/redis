<?php

use AloneWebMan\Redis\Facade;

/**
 * @param string|int|null $name 连接名
 * @return Facade
 */
function alone_redis(string|int|null $name = null): Facade {
    $app = config('plugin.alone.redis.app', []);
    $config = $app['config'] ?? [];
    $key = key($config);
    $name = (!empty($name) ? $name : $app['name'] ?? null);
    return alone_redis_client(($name ? ($config[$name] ?? ($config[$key] ?? [])) : ($config[$key] ?? [])));
}

/**
 * 自定配置连接
 * @param array $config 自定配置
 * @return Facade
 */
function alone_redis_client(array $config): Facade {
    return new Facade($config);
}

/**
 * @param string $key      Redis hash key
 * @param string $field    Hash 字段名
 * @param float  $amount   正数加/负数扣/0返回当前余额
 * @param int    $multiple 转换倍数
 * @return float|null    成功返回当前余额，失败返回 null
 * @return float|null
 */
function alone_redis_balance(string $key, string $field, float $amount = 0, int $multiple = 1000000): ?float {
    return alone_redis()->balance($key, $field, $amount, $multiple);
}

/**
 * 设置队列
 * @param string|int $key
 * @param mixed      $val
 * @param bool       $type true=左入,false=右入
 * @return bool|int
 */
function alone_redis_set(string|int $key, mixed $val, bool $type = true): bool|int {
    return $type ? alone_redis()->queueSet($key, $val) : alone_redis()->queueRightSet($key, $val);
}

/**
 * 获取队列
 * @param string|int $key
 * @param int        $int
 * @param callable   $callable
 * @param bool       $back 出错时是否扔回队列
 * @param bool       $type true=右出,false=左出
 * @return bool|int
 */
function alone_redis_get(string|int $key, int $int, callable $callable, bool $back = true, bool $type = true): bool|int {
    $error = function($queue, $redis, $err) use ($key, $back, $type) {
        ($back) && alone_redis_set($key, $queue, $type);
        $redis->hSet("queue_get_error:" . $key, date("Y-m-d H:i:s") . "_" . substr(md5($queue), 8, 16), $err);
    };
    return $type ? alone_redis()->queueGets($key, $int, $callable, $error) : alone_redis()->queueRightGets($key, $int, $callable, $error);
}

/**
 * 设置有序
 * @param string|int $key
 * @param mixed      $val
 * @param int        $time
 * @return bool|int
 */
function alone_redis_add(string|int $key, mixed $val, int $time = 0): bool|int {
    return alone_redis()->zAdd($key, $val, $time);
}

/**
 * 获取有序
 * @param string|int $key
 * @param callable   $callable
 * @param bool       $back 出错时是否扔回队列
 * @param int        $time
 * @return bool|int
 */
function alone_redis_seek(string|int $key, callable $callable, bool $back = true, int $time = 0): bool|int {
    $error = function($queue, $time, $redis, $err) use ($key, $back) {
        ($back) && alone_redis_add($key, $queue);
        $redis->hSet("queue_get_error:" . $key, date("Y-m-d H:i:s") . "_" . substr(md5($queue), 8, 16), $err);
    };
    return alone_redis()->zGets($key, $callable, $error, $time);
}