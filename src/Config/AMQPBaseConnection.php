<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 16:40
 */

namespace MaxSky\AMQP\Config;

use AMQPConnection;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;

class AMQPBaseConnection {

    /** @var AbstractConnection|AMQPConnection */
    private $connection = null;

    /**
     * @param AMQPConfig $config
     *
     * @throws AMQPConnectionException
     */
    public function __construct(AMQPConfig $config) {
        if (extension_loaded('amqp')) {
            $this->createConnectionByExt($config);
        } else {
            $this->createConnectionByLib($config);
        }
    }

    /**
     * @return AMQPConnection|AbstractConnection
     * @throws AMQPConnectionException
     */
    public function getConnection() {
        if (!$this->connection) {
            throw new AMQPConnectionException('Connection not created');
        }

        return $this->connection;
    }

    /**
     * @param AMQPConfig $config
     *
     * @return void
     * @throws AMQPConnectionException
     */
    private function createConnectionByExt(AMQPConfig $config) {
        try {
            $this->connection = (new AMQPConnection(array_merge([
                'host' => $config->host,
                'port' => $config->port,
                'vhost' => $config->vhost,
                'login' => $config->user,
                'password' => $config->password,
                'connection_name' => $config->connection_name
            ], $config->connect_options)))->connect();
        } catch (\AMQPConnectionException $e) {
            throw new AMQPConnectionException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }

    /**
     * @param AMQPConfig $config
     *
     * @return void
     * @throws AMQPConnectionException
     */
    private function createConnectionByLib(AMQPConfig $config) {
        $connectionConfig = new AMQPConnectionConfig();

        $connectionConfig->setHost($config->host);
        $connectionConfig->setPort((int)$config->port);
        $connectionConfig->setVhost($config->vhost);
        $connectionConfig->setUser($config->user);
        $connectionConfig->setPassword($config->password);
        $connectionConfig->setLocale($config->locale);
        $connectionConfig->setConnectionName($config->connection_name);

        foreach ($config->connect_options as $key => $value) {
            $method = 'set' . ucfirst($key);

            if (!method_exists($connectionConfig, $method)) {
                throw new AMQPConnectionException("Unsupported connection config: $key. Please check supported config from class PhpAmqpLib\Connection\AMQPConnectionConfig");
            }

            $connectionConfig->$method($value);
        }

        $this->connection = AMQPConnectionFactory::create($connectionConfig);

        if (!$this->connection->isConnected()) {
            throw new AMQPConnectionException('Cannot connect to the broker.');
        }
    }
}
