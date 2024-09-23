<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 18
 * Time: 16:35
 */

namespace MaxSky\AMQP\Queue;

use AMQPChannel;
use AMQPConnectionException;
use AMQPException;
use AMQPQueue;
use MaxSky\AMQP\Exception\AMQPQueueException;

class AMQPReceiveByExtension extends AbstractReceiveMessage {

    public function handle() {

    }

    public function receive(string $handler, $data, ?string $queue_name = 'default', bool $transaction = false) {
        // TODO: Implement receive() method.
    }

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    protected function prepare() {
        try {
            $this->channel = new AMQPChannel($this->connection);

//            $this->exchange = new AMQPExchange($this->channel);
//            $this->exchange->setFlags(AMQP_DURABLE);
//
//            $this->exchange_name = $this->config->connection_name;

            $retry = $this->options['type'] === 'retry';

//            if ($retry) {
//                $this->exchange_name .= '.retry';
//            } else if ($this->options['delay']) {
//                $this->exchange_name .= '.delay';
//
//                $this->exchange->setType(AMQPExchangeType::DELAYED);
//                $this->exchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
//            } else {
//                $this->exchange->setType(AMQPExchangeType::TOPIC);
//            }
//
//            $this->exchange->setName($this->exchange_name);
//            $this->exchange->declare();

            foreach ($this->options['queues'] as $queue_name) {
                $queue = new AMQPQueue($this->channel);

                if ($retry) {
                    $queue_name .= '.retry';
                }

                $queue->setName($queue_name);
                $queue->setFlags(AMQP_DURABLE);
                $queue->declare();
            }
        } catch (AMQPException $e) {
            $this->connection->disconnect();

            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }
}
