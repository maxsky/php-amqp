<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 19:01
 */

namespace MaxSky\AMQP\Queue;

use AMQPConnection;
use Carbon\Carbon;
use DateTimeInterface;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Handler\MessageHandlerInterface;
use PhpAmqpLib\Connection\AbstractConnection;

abstract class AbstractSendMessage {

    /** @var AbstractConnection|AMQPConnection */
    protected $connection;

    /** @var string */
    protected $connectionName;

    public function __construct($connection, string $connection_name) {
        $this->connection = $connection;
        $this->connectionName = $connection_name;
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
     * 参数过滤
     *
     * @param string                     $handler 队列处理器
     * @param mixed                      $data    队列数据
     * @param int|DateTimeInterface|null $delay
     *
     * @return void
     * @throws AMQPQueueException
     */
    protected function paramsFilter(string $handler, $data, &$delay = 0): void {
        if (!class_exists($handler) || !is_a($handler, MessageHandlerInterface::class)) {
            throw new AMQPQueueException('Handler not exist or not implement from base Handler interface.');
        }

        if (empty($data)) {
            throw new AMQPQueueException('Send data could not be empty.');
        }

        if ($delay instanceof DateTimeInterface) {
            $delay = Carbon::now()->diffInSeconds($delay, false);
        }

        $delay = (int)$delay;
    }
}
