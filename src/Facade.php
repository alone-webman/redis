<?php

namespace AloneWebMan\Redis;

use Redis;

class Facade {
    //是否使用新连接
    protected bool $update = false;
    //选择数据库
    protected int $db = 0;
    //redis连接
    public Redis|null $redis = null;
    //配置
    public array $config = [
        //tcp tls ssl
        'scheme'         => 'tcp',
        //服务器的主机名或 IP 地址
        'host'           => '127.0.0.1',
        //服务器的端口号，默认是 6379
        'port'           => 6379,
        //服务器redis密码
        'password'       => null,
        //选择数据库
        'database'       => 0,
        //连接超时时间，以秒为单位。默认值为 0.0，表示无限制。
        'timeout'        => 3,
        //用于持久连接的标识符。如果提供此参数，连接将被视为持久连接
        'persistent'     => false,
        //如果连接失败，重试的间隔时间（以毫秒为单位）。默认值为 0，表示不重试
        'retry_interval' => 0,
        //读取超时时间，以秒为单位。默认值为 0，表示无限制
        'read_timeout'   => 0,
        //选项
        'options'        => [],
        //前缀
        'prefix'         => '',
    ];

    /**
     * 添加元素到 Set
     * @param string|int $key   Redis key
     * @param string|int $value 元素值
     * @param int        $time  TTL（秒），0 表示不设置
     * @param bool       $force 是否每次刷新 TTL
     * @return bool true=新增, false=已存在
     */
    public function setVal(string|int $key, string|int $value, int $time = 0, bool $force = false): bool {
        $keys = $this->connect()->getKey($key);
        $added = $this->redis->sAdd($keys, $this->setValue($value));
        $res = $added == 1;
        if ($res && $time > 0) {
            $ttl = $this->redis->ttl($keys);
            if ($ttl < 0 || $force) {
                $this->redis->expire($keys, $time);
            }
        }
        return $res;
    }

    /**
     * 获取 Set 中所有元素
     * @param string|int $key Redis key
     * @return array 元素数组，如果 key 不存在返回空数组
     */
    public function getVal(string|int $key): array {
        $keys = $this->connect()->getKey($key);
        $items = $this->redis->sMembers($keys);
        return is_array($items) ? $items : [];
    }

    /**
     * 判断元素是否存在于 Set
     * @param string|int $key   Redis key
     * @param string|int $value 元素值
     * @return bool true=存在, false=不存在
     */
    public function isVal(string|int $key, string|int $value): bool {
        $keys = $this->connect()->getKey($key);
        return $this->redis->sIsMember($keys, $this->setValue($value)) === true;
    }

    /**
     * @param array $config
     */
    public function __construct(array $config = []) {
        $this->config = array_merge($this->config, $config);
    }

    /**
     * 连续
     * @param array $config
     * @return static
     */
    public static function link(array $config = []): static {
        $redis = new static($config);
        $redis->redis();
        return $redis;
    }

    /**
     * @return static
     */
    public function connect(): static {
        if (empty($this->redis) || !empty($this->update)) {
            $this->redis();
        }
        $this->redis->select($this->db ?: 0);
        return $this;
    }

    /**
     * 关闭连接
     * @return $this
     */
    public function close(): static {
        if (!empty($this->redis)) {
            $this->redis->close();
            $this->redis = null;
        }
        return $this;
    }

    /**
     * 选择数据库
     * @param int $db
     * @return $this
     */
    public function select(int $db = 0): static {
        $this->db = $db;
        return $this;
    }

    /**
     * 是否更新连接
     * @param bool $update
     * @return $this
     */
    public function update(bool $update): static {
        $this->update = $update;
        return $this;
    }

    /**
     * 获取key
     * @param string|int $key
     * @return string|int
     */
    public function getKey(string|int $key): string|int {
        return (!empty($prefix = ($this->getConfig('prefix'))) ? ($prefix . ":") : "") . $key;
    }

    /**
     * 队列数量
     * @param string|int $key
     * @return int
     */
    public function queueCount(string|int $key): int {
        $keys = $this->connect()->getKey($key);
        $len = $this->redis->lLen($keys);
        return is_numeric($len) && $len > 0 ? $len : 0;
    }

    /**
     * 设置 左侧队列
     * @param string|int $key
     * @param mixed      $val
     * @return bool|int
     */
    public function queueSet(string|int $key, mixed $val): bool|int {
        $keys = $this->connect()->getKey($key);
        return $this->redis->lpush($keys, $this->setValue($val));
    }

    /**
     * 获取 右侧队列
     * @param string|int $key
     * @return bool|string|int
     */
    public function queueGet(string|int $key): bool|string|int {
        $keys = $this->connect()->getKey($key);
        return $this->redis->rpop($keys);
    }

    /**
     * @param string|int $key
     * @param int        $index 获取第几个,从1起
     * @return bool|string|int
     */
    public function lIndex(string|int $key, int $index): bool|string|int {
        $keys = $this->connect()->getKey($key);
        return $this->redis->lindex($keys, $index);
    }


    /**
     * 处理 左入右出 队列
     * @param string|int    $key
     * @param int           $int      获取数量
     * @param callable      $callable 处理包
     * @param callable|null $error    处理包
     * @param int           $j
     * @return int
     */
    public function queueGets(string|int $key, int $int, callable $callable, callable|null $error = null, int $j = 0): int {
        if (!empty($this->exists($key))) {
            for ($i = 1; $i <= $int; $i++) {
                if (!empty($queue = $this->queueGet($key))) {
                    ++$j;
                    try {
                        $callable(static::isJson($queue) ?: $queue, $this);
                    } catch (\Throwable|\Exception $e) {
                        $err = ['code' => $e->getCode(), 'line' => $e->getLine(), 'file' => $e->getFile(), 'msg' => $e->getMessage()];
                        $error && $error($queue, $this, $err, $e);
                    }
                }
            }
        }
        return $j;
    }

    /**
     * 自增 可增大数
     * @param string|int $key
     * @param int        $value
     * @return int|float
     */
    public function incrBy(string|int $key, int $value = 1): int|float {
        $keys = $this->connect()->getKey($key);
        return $this->redis->incrBy($keys, $value);
    }

    /**
     * 自减 可增减数
     * @param string|int $key
     * @param int        $value
     * @return int|float
     */
    public function decrBy(string|int $key, int $value = 1): int|float {
        $keys = $this->connect()->getKey($key);
        return $this->redis->decrBy($keys, $value);
    }

    /**
     * 设置 有序列表
     * @param string|int $key
     * @param mixed      $val
     * @param int        $time
     * @return mixed
     */
    public function zAdd(string|int $key, mixed $val, int $time = 0): mixed {
        $keys = $this->connect()->getKey($key);
        return $this->redis->zadd($keys, ($time ?: time()), $this->setValue($val));
    }

    /**
     * 原子增减 Redis 余额（高并发安全，HINCRBYFLOAT 浮点原生）
     * @param string $key      Redis hash key
     * @param string $field    Hash 字段名,可以使用会员号
     * @param float  $amount   正数加/负数扣/0返回当前余额
     * @param int    $multiple 转换倍数
     * @return float|null    成功返回当前余额，失败返回 null
     */
    public function balance(string $key, string $field, float $amount = 0, int $multiple = 1000000): ?float {
        $keys = $this->connect()->getKey($key);
        if ($amount == 0) {
            $cur = $this->redis->hGet($keys, $field) ?? 0;
            return (float) ($cur / $multiple);
        }
        $result = $this->redis->eval(<<<LUA
local key = KEYS[1]
local field = ARGV[1]
local amount = tonumber(ARGV[2])
local cur = redis.call("HGET", key, field)
if not cur then cur = "0" end
cur = tonumber(cur)
if amount < 0 and cur + amount < 0 then
    return -1
end
return redis.call("HINCRBY", key, field, amount)
LUA, [$keys, $field, (int) ($amount * $multiple)], 1);
        return $result === -1 ? null : ((float) ($result / $multiple));
    }

    /**
     * 获取 有序列表
     * @param string|int $key
     * @param int        $time
     * @return array
     */
    public function zGet(string|int $key, int $time = 0): array {
        $keys = $this->connect()->getKey($key);
        return $this->redis->zrangebyscore($keys, '-inf', ($time ?: time()), ['WITHSCORES' => true]);
    }

    /**
     * @param string|int $key
     * @param int        $index 获取第几个,从1起
     * @return array|null
     */
    public function zIndex(string|int $key, int $index): ?array {
        $keys = $this->connect()->getKey($key);
        $items = $this->redis->zRange($keys, $index - 1, $index - 1, true);
        return (is_array($items) && !empty($item = key($items))) ? ['key' => $item, 'value' => $items[$item]] : null;
    }

    /**
     * 处理 有序列表
     * @param string|int    $key
     * @param callable      $callable
     * @param callable|null $error
     * @param int           $time
     * @param int           $j
     * @return int
     */
    public function zGets(string|int $key, callable $callable, callable|null $error = null, int $time = 0, int $j = 0): int {
        if (!empty($array = $this->zGet($key, $time))) {
            foreach ($array as $queue => $times) {
                if ($this->zDel($key, $queue) > 0) {
                    ++$j;
                    try {
                        $callable(static::isJson($queue) ?: $queue, $times, $this);
                    } catch (\Throwable|\Exception $e) {
                        $err = ['code' => $e->getCode(), 'line' => $e->getLine(), 'file' => $e->getFile(), 'msg' => $e->getMessage()];
                        $error && $error($queue, $times, $this, $err, $e);
                    }
                }
            }
        }
        return $j;
    }

    /**
     * 删除指定 有序列表
     * @param string|int $key
     * @param mixed      $val
     * @return mixed
     */
    public function zDel(string|int $key, mixed $val = null): mixed {
        $keys = $this->connect()->getKey($key);
        return ($val ? $this->redis->zrem($keys, $val) : $this->redis->del($keys));
    }

    /**
     * 设置 缓存
     * @param int|string $key
     * @param int|string $name
     * @param mixed      $val
     * @param int        $time
     * @return mixed
     */
    public function hSet(int|string $key, int|string $name, mixed $val, int $time = 0): mixed {
        $keys = $this->connect()->getKey($key);
        $exists = true;
        if ($time > 0) {
            $exists = $this->exists($key);
        }
        $res = $this->redis->hmset($keys, [$name => $this->setValue($val)]);
        if (empty($exists)) {
            $this->expire($key, $time);
        }
        return $res;
    }

    /**
     * 获取 缓存
     * @param int|string $key
     * @param int|string $name
     * @param mixed      $def
     * @return mixed
     */
    public function hGet(int|string $key, int|string $name, mixed $def = ''): mixed {
        $keys = $this->connect()->getKey($key);
        return $this->getValue($this->redis->hget($keys, $name), $def);
    }

    /**
     * 删除 缓存
     * @param int|string $key
     * @param int|string $name
     * @return Redis|int|bool
     */
    public function hDel(int|string $key, int|string $name = ''): Redis|int|bool {
        $keys = $this->connect()->getKey($key);
        return $name ? $this->redis->hdel($keys, $name) : $this->redis->del($keys);
    }

    /**
     * 设置
     * @param string|int $key
     * @param mixed      $val
     * @param int        $time
     * @return mixed
     */
    public function set(string|int $key, mixed $val, int $time = 0): mixed {
        $keys = $this->connect()->getKey($key);
        $res = $this->redis->set($keys, $this->setValue($val));
        if ($time > 0) {
            $this->expire($key, $time);
        }
        return $res;
    }

    /**
     * 获取
     * @param string|int $key
     * @param mixed      $def
     * @return mixed
     */
    public function get(string|int $key, mixed $def = ''): mixed {
        $keys = $this->connect()->getKey($key);
        return $this->getValue($this->redis->get($keys), $def);
    }

    /**
     * 删除
     * @param string|int $key
     * @return Redis|int|bool
     */
    public function del(string|int $key): Redis|int|bool {
        $keys = $this->connect()->getKey($key);
        return $this->redis->del($keys);
    }

    /**
     * 设置有效时间
     * @param string|int $key
     * @param int        $time
     * @return mixed
     */
    public function expire(string|int $key, int $time): mixed {
        $keys = $this->connect()->getKey($key);
        return $this->redis->expire($keys, $time);
    }

    /**
     * key是否存在
     * @param string|int $key
     * @return mixed
     */
    public function exists(string|int $key): mixed {
        $keys = $this->connect()->getKey($key);
        return $this->redis->exists($keys);
    }

    /**
     * 删除key下面全部key
     * @param string|int|bool $key    true=全清空
     * @param bool            $prefix 是否加前缀
     * @param int             $count
     * @return int
     */
    public function delete(string|int|bool $key, bool $prefix = true, int $count = 0): int {
        $this->connect();
        if ($key === true) {
            return $this->redis->flushDB() === true ? 1 : 0;
        }
        $list = $this->redis->keys(($prefix ? $this->getKey($key) : $key) . ':*');
        if (!empty($list)) {
            foreach ($list as $item) {
                ++$count;
                $this->redis->del($item);
            }
        }
        return $count;
    }

    /**
     * 同名称和标识,如:api/login,user
     * @param string   $name      锁名称
     * @param string   $uuid      锁标积 如同一个名称要分标识
     * @param callable $callback  执行包(true=可执行,false=不能访问)
     * @param int      $lock_time 锁的超时时间（5秒）
     * @param int      $flag_time 幂等标志 (60秒内防止重复提交）
     * @return mixed
     */
    public function lockup(string $name, string $uuid, callable $callback, int $lock_time = 5, int $flag_time = 60): mixed {
        $flagKey = "flag:$name";
        $this->redis->set($flagKey, 1, ['EX' => $flag_time]);
        $lockKey = "lock:$name";
        $rep = $this->redis->set($lockKey, $uuid, ['NX', 'EX' => $lock_time]);
        if ($rep === true) {
            $res = $callback(true);
            $script = 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end';
            $this->redis->eval($script, [$lockKey, $uuid], 1);
        } else {
            $res = $callback(false);
        }
        return $res;
    }

    /**
     * 设置排他锁
     * @param string|int $key
     * @param int        $time
     * @param mixed      $val
     * @return mixed
     */
    public function setNx(string|int $key, int $time, mixed $val = 1): mixed {
        $keys = $this->connect()->getKey($key);
        return $this->redis->set($keys, $val, ['nx', 'ex' => $time]);
    }

    /**
     * redis 排他锁 执行
     * @param string|int    $key      唯一标识
     * @param callable      $callable 执行包
     * @param callable|bool $closure  超时的时候处理,false=不处理,true=运行执行包,callable($callable)=自定执行包
     * @param int           $timeout  有效时间,执行的最长等待时间 秒
     * @param int           $wait     间隔等待时间 微秒
     * @return mixed
     */
    public function lock(string|int $key, callable $callable, callable|bool $closure = false, int $timeout = 5, int $wait = 100): mixed {
        $keys = $this->connect()->getKey($key);
        $startTime = time();
        while (true) {
            if ($this->setNx($key, $timeout)) {
                break;
            } else {
                usleep($wait * 10000);
                if ((time() - $startTime) >= $timeout) {
                    return (!empty($closure) ? (is_callable($closure) ? $closure($callable) : $callable()) : $closure);
                }
                usleep($wait * 10000);
            }
        }
        $res = $callable();
        if ($this->exists($key)) {
            $this->redis->del($keys);
        }
        return $res;
    }

    /**
     * 设置 右侧队列
     * @param string|int $key
     * @param mixed      $val
     * @return bool|int
     */
    public function queueRightSet(string|int $key, mixed $val): bool|int {
        $keys = $this->connect()->getKey($key);
        return $this->redis->rpush($keys, $this->setValue($val));
    }

    /**
     * 获取 左侧队列
     * @param string|int $key
     * @return bool|string|int
     */
    public function queueRightGet(string|int $key): bool|string|int {
        $keys = $this->connect()->getKey($key);
        return $this->redis->lpop($keys);
    }

    /**
     * 处理 右入左出 队列
     * @param string|int    $key
     * @param int           $int      获取数量
     * @param callable      $callable 处理包
     * @param callable|null $error    处理包
     * @param int           $j
     * @return int
     */
    public function queueRightGets(string|int $key, int $int, callable $callable, callable|null $error = null, int $j = 0): int {
        if (!empty($this->exists($key))) {
            for ($i = 1; $i <= $int; $i++) {
                if (!empty($queue = $this->queueRightGet($key))) {
                    ++$j;
                    try {
                        $callable(static::isJson($queue) ?: $queue, $this);
                    } catch (\Exception|\Throwable $e) {
                        $error && $error($queue, $this, $e);
                    }
                }
            }
        }
        return $j;
    }

    /**
     * 原生redis链接
     * @return Redis
     */
    protected function redis(): Redis {
        $this->redis = new Redis();
        call_user_func_array([$this->redis, $this->getConfig('redis', 'connect')], [
            (!empty($scheme = $this->getConfig('scheme', '')) ? ($scheme . "://") : "") . $this->getConfig('host', '127.0.0.1'),
            $this->getConfig('port', 6379),
            $this->getConfig('timeout', 5),
            $this->getConfig('persistent') ?: null,
            $this->getConfig('retry_interval', 0),
            $this->getConfig('read_timeout', 0),
            $this->getConfig('options', [])
        ]);
        if (!empty($password = $this->getConfig('password'))) {
            $this->redis->auth($password);
        }
        $this->select($this->getConfig('database', 0));
        $this->redis->select($this->db ?? 0);
        return $this->redis;
    }


    /**
     * 获取配置
     * @param string|int|null $key
     * @param mixed           $default
     * @return mixed
     */
    protected function getConfig(string|int|null $key, mixed $default = null): mixed {
        return static::getArr($this->config, $key, $default);
    }


    /**
     * 保存数据
     * @param mixed $val
     * @return mixed
     */
    protected function setValue(mixed $val): mixed {
        $val = is_callable($val) ? $val() : $val;
        return (is_array($val) ? static::json($val) : $val);
    }

    /**
     * 获取数据
     * @param mixed $val
     * @param mixed $def
     * @return mixed
     */
    protected function getValue(mixed $val, mixed $def = ''): mixed {
        return (static::isJson($val) ?: $val) ?? $def;
    }

    /**
     * 数组转Json
     * @param array|object $array
     * @param bool         $int 是否数字检查
     * @return false|string
     */
    public static function json(array|object $array, bool $int = true): bool|string {
        return $int ? json_encode($array, JSON_NUMERIC_CHECK + JSON_UNESCAPED_UNICODE + JSON_UNESCAPED_SLASHES) : json_encode($array, JSON_UNESCAPED_UNICODE + JSON_UNESCAPED_SLASHES);
    }

    /**
     * 判断字符串是否json,返回array
     * @param mixed     $json
     * @param bool|null $associative
     * @param int       $depth
     * @param int       $flags
     * @return mixed
     */
    public static function isJson(mixed $json, bool $associative = true, int $depth = 512, int $flags = 0): mixed {
        $json = json_decode((is_string($json) ? ($json ?: '') : ''), $associative, $depth, $flags);
        return (($json && is_object($json)) || (is_array($json) && $json)) ? $json : [];
    }

    /**
     * 通过a.b.c.d获取数组内容
     * @param array|null      $array   要取值的数组
     * @param string|null|int $key     支持aa.bb.cc.dd这样获取数组内容
     * @param mixed           $default 默认值
     * @param string          $symbol  自定符号
     * @return mixed
     */
    protected static function getArr(array|null $array, string|null|int $key = null, mixed $default = null, string $symbol = '.'): mixed {
        if (isset($key)) {
            if (isset($array[$key])) {
                $array = $array[$key];
            } else {
                $symbol = $symbol ?: '.';
                $arr = explode($symbol, trim($key, $symbol));
                foreach ($arr as $v) {
                    if (isset($v) && isset($array[$v])) {
                        $array = $array[$v];
                    } else {
                        $array = $default;
                        break;
                    }
                }
            }
        }
        return $array ?? $default;
    }

    public function __destruct() {
        $this->close();
    }
}