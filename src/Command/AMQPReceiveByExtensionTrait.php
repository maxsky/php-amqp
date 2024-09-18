<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 18
 * Time: 16:35
 */

namespace MaxSky\AMQP\Command;

use AMQPChannel;
use AMQPConnection;
use AMQPConnectionException;
use AMQPException;
use AMQPExchange;
use AMQPQueue;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPQueueException;

trait AMQPReceiveByExtensionTrait {

    private $channel;
    private $exchange;
    private $queue;

    /**
     * @return void
     * @throws AMQPQueueException
     * @throws AMQPConnectionException
     */
    protected function handleMessage() {
        $this->initExtensionExchangeQueue();

    }

    /**
     * @return void
     * @throws AMQPQueueException
     * @throws AMQPConnectionException
     */
    private function initExtensionExchangeQueue() {
        try {
            $this->connection->connect();

            $this->channel = new AMQPChannel($this->connection);

            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setFlags(AMQP_DURABLE);

            $exchangeName = $this->config->connection_name;

            if ($this->delay) {
                $exchangeName .= '.delay';

                $this->exchange->setType(AMQPExchangeType::DELAYED);
                $this->exchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
            } else {
                $this->exchange->setType(AMQPExchangeType::TOPIC);
            }

            $this->exchange->setName($exchangeName);
            $this->exchange->declare();

            $queueSuffix = '';
            $args = [];

            if ($this->type === 'retry') {
                $queueSuffix = '.retry';

                $args = array_merge($args, [
                    'x-dead-letter-exchange' => EXCHANGE_NORMAL
                ]);
            }

            foreach ($this->queues as $item) {
                $queue = new AMQPQueue($this->channel);

                $queue->setName($item . $queueSuffix);
                $queue->setFlags(AMQP_DURABLE);
                $queue->setArguments($args);
                $queue->declare();

                $queue->bind($exchangeName);
            }
        } catch (AMQPException $e) {
            $this->connection->disconnect();

            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }
}
