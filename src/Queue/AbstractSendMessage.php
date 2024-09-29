<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 19:01
 */

namespace MaxSky\AMQP\Queue;

use AMQPConnection;
use AMQPExchange;
use AMQPQueue;
use Carbon\Carbon;
use DateTimeInterface;
use Exception;
use MaxSky\AMQP\Config\AMQPBaseConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Handler\MessageHandlerInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;

abstract class AbstractSendMessage {

    /** @var AbstractConnection|AMQPConnection */
    protected $connection;

    /** @var AMQPBaseConfig */
    protected $config;

    /** @var \AMQPChannel|AMQPChannel */
    protected $channel;
    /** @var AMQPExchange */
    protected $exchange;

    protected $exchange_name;

    /** @var AMQPQueue */
    protected $queue;
    /** @var AMQPQueue */
    protected $retryQueue;

    protected $delay_msec = 0;

    /**
     * @param AbstractConnection|AMQPConnection $connection
     * @param AMQPBaseConfig                    $config
     *
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    public function __construct($connection, AMQPBaseConfig $config) {
        $this->connection = $connection;
        $this->config = $config;

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
    abstract public function send(string  $handler, $data,
                                  ?string $queue_name = 'default', bool $transaction = false);

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    abstract protected function prepare();

    /**
     * @param int|string|DateTimeInterface $time
     *
     * @return AbstractSendMessage
     */
    public function delay($time): AbstractSendMessage {
        if (is_string($time) || ($time instanceof DateTimeInterface)) {
            $time = Carbon::now()->diffInMicroseconds($time);
        }

        $time = (int)$time;

        if ($time < 0) {
            $time = 0;
        }

        $this->delay_msec = $time;

        return $this;
    }

    /**
     * 参数过滤
     *
     * @param string      $handler    队列处理器
     * @param mixed       $data       队列数据
     * @param string|null $queue_name 队列名称
     *
     * @return void
     * @throws AMQPQueueException
     */
    protected function paramsFilter(string $handler, $data, ?string $queue_name): void {
        if (!class_exists($handler) || !is_a($handler, MessageHandlerInterface::class, true)) {
            throw new AMQPQueueException('Handler not exist or not implement from base Handler interface.');
        }

        if (empty($data)) {
            throw new AMQPQueueException('Send data could not be empty.');
        }

        if (!$queue_name) {
            throw new AMQPQueueException('Queue name could not be empty.');
        }
    }
}
