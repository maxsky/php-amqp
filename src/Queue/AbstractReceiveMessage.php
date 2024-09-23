<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 23
 * Time: 18:01
 */

namespace MaxSky\AMQP\Queue;

use AMQPConnection;
use AMQPExchange;
use Exception;
use MaxSky\AMQP\Config\AMQPConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;

abstract class AbstractReceiveMessage {

    /** @var AbstractConnection|AMQPConnection */
    protected $connection;

    /** @var AMQPConfig */
    protected $config;

    protected $options;

    /** @var \AMQPChannel|AMQPChannel */
    protected $channel;
    /** @var AMQPExchange */
    protected $exchange;

    protected $exchange_name;

    /**
     * @param AbstractConnection|AMQPConnection $connection
     * @param AMQPConfig                        $config
     * @param array                             $options
     *
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    public function __construct($connection, AMQPConfig $config, array $options) {
        $this->connection = $connection;
        $this->config = $config;
        $this->options = $options;

        try {
            if ($this->connection instanceof AMQPConnection) {
                $connection->connect();
            }
        } catch (Exception $e) {
            throw new AMQPConnectionException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }

        $this->prepare();
    }

    /**
     * @param string      $handler
     * @param mixed       $data
     * @param string|null $queue_name
     * @param bool        $transaction
     *
     * @return void
     */
    abstract public function receive(string  $handler, $data,
                                     ?string $queue_name = 'default', bool $transaction = false);

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    abstract protected function prepare();
}
