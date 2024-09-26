<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 26
 * Time: 14:04
 */

namespace MaxSky\AMQP\Config;

class AMQPConfig {

    /** @var AMQPBaseConfig */
    private $config;

    /**
     * @param AMQPBaseConfig|null $config
     */
    public function __construct(?AMQPBaseConfig $config = null) {
        if (!$config) {
            $config = new AMQPBaseConfig();

            $config->connection_name = config('amqp.connection.name');
            $config->host = config('amqp.connection.host');
            $config->port = config('amqp.connection.port');
            $config->user = config('amqp.connection.user');
            $config->password = config('amqp.connection.password');
            $config->vhost = config('amqp.connection.vhost');

            $config->queue_ttl = config('amqp.arguments.queue_ttl');

            $config->connect_options = config('amqp.connection.options');
        }

        $this->config = $config;
    }

    public function setConfig(AMQPBaseConfig $config) {
        $this->config = $config;
    }

    /**
     * @return AMQPBaseConfig
     */
    public function getConfig(): AMQPBaseConfig {
        return $this->config;
    }
}
