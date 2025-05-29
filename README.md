# 原生redis

### 安装仓库

```text
composer require alone-webman/redis
```

### 配置 `config/plugin/alone/apple/app.php`

```php
<?php
return [
    'enable' => true,
    /*
     * 默认连接名
     */
    'name'   => 'alone',
    /*
     * 配置列表
     */
    'config' => [
        'alone' => [
            /*
             * tcp tls ssl
             */
            'scheme'         => 'tcp',
            /*
             * 服务器的主机名或 IP 地址
             */
            'host'           => '127.0.0.1',
            /*
             * 服务器的端口号，默认是 6379
             */
            'port'           => 6379,
            /*
             * 服务器redis密码
             */
            'password'       => null,
            /*
             * 选择数据库
             */
            'database'       => 0,
            /*
             * Key前缀
             */
            'prefix'         => '',
            /*
             * 连接超时时间，以秒为单位。默认值为 0.0，表示无限制。
             */
            'timeout'        => 3,
            /*
             * 用于持久连接的标识符。如果提供此参数，连接将被视为持久连接
             */
            'persistent'     => false,
            /*
             * 如果连接失败，重试的间隔时间（以毫秒为单位）。默认值为 0，表示不重试
             */
            'retry_interval' => 0,
            /*
             * 读取超时时间，以秒为单位。默认值为 0，表示无限制
             */
            'read_timeout'   => 0,
            /*
             * 选项
             */
            'options'        => []
        ]
    ]
];
```

### 自定配置连接

```php
alone_redis_client(array $config)
```

### 使用app.php配置连接

```php
alone_redis(string|int|null $name = null)
```

### 使用默认配置把消息加入队列

```php
alone_redis_set(string|int $key, mixed $val, bool $type = true)
```

### 使用默认配置获取队列消息

```php
alone_redis_get(string|int $key, int $int, callable $callable, bool $back = true, bool $type = true)
```

### 使用默认配置把消息加入队列(有序列表)

```php
alone_redis_add(string|int $key, mixed $val, int $time = 0)
```

### 使用默认配置获取队列消息(有序列表)

```php
alone_redis_seek(string|int $key, callable $callable, bool $back = true, int $time = 0)
```