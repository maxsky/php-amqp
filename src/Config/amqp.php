<?php

return [
    'connection' => [
        'name' => config('app.name'),
        'host' => env('AMQP_HOST', '127.0.0.1'),
        'port' => (int)env('AMQP_PORT', 5672),
        'user' => env('AMQP_USER', 'guest'),
        'password' => env('AMQP_PASSWORD', 'guest'),
        'vhost' => env('AMQP_VHOST', '/'),
        'options' => [

        ]
    ],
    'arguments' => [
        'queue_ttl' => (int)env('AMQP_QUEUE_TTL', 5000)
    ]
];
