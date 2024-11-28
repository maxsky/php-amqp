<?php

return [
    'connection' => [
        'name' => env('APP_NAME', 'AMQP-ForRabbitMQ'),
        'host' => env('AMQP_HOST', '127.0.0.1'),
        'port' => (int)env('AMQP_PORT', 5672),
        'user' => env('AMQP_USER', 'guest'),
        'password' => env('AMQP_PASSWORD', 'guest'),
        'vhost' => env('AMQP_VHOST', '/'),
        'options' => [
            'connect_timeout' => 10.0,
            'read_timeout' => 0.0,  // php-amqp extension please set to 0
            'write_timeout' => 10.0,
            'heartbeat' => 60
        ]
    ],
    'arguments' => [
        'queue_ttl' => (int)env('AMQP_QUEUE_TTL', 0)
    ]
];
