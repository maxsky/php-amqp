<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 19:01
 */

namespace MaxSky\AMQP\Queue;

use AMQPChannel;
use AMQPConnection;
use AMQPExchange;
use AMQPQueue;
use Carbon\Carbon;
use DateTimeInterface;
use Exception;
use MaxSky\AMQP\Config\AMQPConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Handler\MessageHandlerInterface;
use PhpAmqpLib\Connection\AbstractConnection;

abstract class AbstractSendMessage {

    /** @var AbstractConnection|AMQPConnection */
    protected $connection;

    /** @var AMQPConfig */
    protected $config;

    /** @var AMQPChannel */
    protected $channel;
    /** @var AMQPExchange */
    protected $exchange;
    /** @var AMQPQueue */
    protected $queue;
    /** @var AMQPQueue */
    protected $retryQueue;

    /**
     * @param AbstractConnection|AMQPConnection $connection
     * @param AMQPConfig                        $config
     *
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    public function __construct($connection, AMQPConfig $config) {
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
     * @param string                     $handler
     * @param mixed                      $data
     * @param string|null                $queue_name
     * @param int|DateTimeInterface|null $delay
     * @param bool                       $transaction
     *
     * @return mixed
     */
    abstract public function send(string  $handler, $data,
                                  ?string $queue_name = 'default', $delay = null, bool $transaction = false);

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    abstract protected function prepare();

    /**
     * 参数过滤
     *
     * @param string                     $handler    队列处理器
     * @param mixed                      $data       队列数据
     * @param string|null                $queue_name 队列名称
     * @param int|DateTimeInterface|null $delay
     *
     * @return void
     * @throws AMQPQueueException
     */
    protected function paramsFilter(string $handler, $data, ?string $queue_name, &$delay = 0): void {
        if (!class_exists($handler) || !is_a($handler, MessageHandlerInterface::class, true)) {
            throw new AMQPQueueException('Handler not exist or not implement from base Handler interface.');
        }

        if (empty($data)) {
            throw new AMQPQueueException('Send data could not be empty.');
        }

        if (!$queue_name) {
            throw new AMQPQueueException('Queue name could not be empty.');
        }

        if ($delay instanceof DateTimeInterface) {
            $delay = Carbon::now()->diffInSeconds($delay, false);
        }

        $delay = (int)$delay;

        if ($delay < 0) {
            $delay = 0;
        }
    }
}
