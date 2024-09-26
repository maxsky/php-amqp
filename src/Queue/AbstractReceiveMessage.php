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
use MaxSky\AMQP\Config\AMQPBaseConnection;
use MaxSky\AMQP\Config\AMQPBaseConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPMessageHandlerException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Handler\MessageHandlerInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;

abstract class AbstractReceiveMessage {

    /** @var AMQPConnection|AbstractConnection */
    protected $connection;

    /** @var AMQPBaseConfig */
    protected $config;

    /** @var array */
    protected $options = [];

    /** @var \AMQPChannel|AMQPChannel */
    protected $channel;

    /** @var AMQPExchange */
    protected $exchange;

    /** @var string */
    protected $exchange_name;

    /** @var array */
    protected $queues = [];

    /**
     * @param AMQPBaseConfig $config
     * @param array          $options
     *
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    public function __construct(AMQPBaseConfig $config, array $options) {
        $this->config = $config;
        $this->options = $options;

        $this->connection = (new AMQPBaseConnection($this->config))->getConnection();

        try {
            if ($this->connection instanceof AMQPConnection) {
                $this->connection->connect();
            } else {
                $this->channel = $this->connection->channel();
            }
        } catch (Exception $e) {
            throw new AMQPConnectionException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }

        $this->prepare();
    }

    /**
     * @return void
     */
    abstract public function receive();

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    abstract protected function prepare();

    /**
     * 队列处理
     *
     * @param string $handler_class 处理类
     * @param mixed  $data          队列数据
     * @param mixed  $result        返回结果
     *
     * @return void
     * @throws AMQPMessageHandlerException
     */
    protected function queueHandle(string $handler_class, $data, &$result = null): void {
        /** @var MessageHandlerInterface $handler */
        $handler = new $handler_class();

        $handler->handle($data, $result);
    }

    /**
     * 失败处理
     *
     * @param string $handler_class
     * @param string $queue_name
     * @param mixed  $data
     * @param array  $headers
     *
     * @return void
     */
    protected function failedHandle(string $handler_class,
                                    string $queue_name, $data, array $headers = []): void {
        /** @var MessageHandlerInterface $handler */
        $handler = new $handler_class();

        $handler->failed([
            'connection' => 'amqp',
            'queue' => $queue_name,
            'data' => json_encode($data),
            'headers' => $headers
        ]);
    }
}
