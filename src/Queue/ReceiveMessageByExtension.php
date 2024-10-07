<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 18
 * Time: 16:35
 */

namespace MaxSky\AMQP\Queue;

use AMQPConnectionException;
use AMQPEnvelope;
use AMQPException;
use AMQPExchange;
use AMQPQueue;
use Exception;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPQueueException;

class ReceiveMessageByExtension extends AbstractReceiveMessage {

    /**
     * @return void
     * @throws AMQPQueueException
     * @throws \MaxSky\AMQP\Exception\AMQPConnectionException
     */
    public function receive() {
        $callback = function (AMQPEnvelope $msg, AMQPQueue $queue) {
            $headers = $msg->getHeaders();

            $body = json_decode($msg->getBody(), true);

            if ($headers['x-exception'] || $headers['x-attempts'] > $this->options['tries']) {
                $body['timestamp'] = $msg->getTimestamp();

                $this->failedHandle($body['handler'], $queue->getName(), $body, $headers);
            } else {
                try {
                    $this->queueHandle($body['handler'], $body['data'], $result);

                    if ($result === false) {
                        $this->failedHandle($body['handler'], $queue->getName(), $body['data'], $headers);
                    }
                } catch (Exception $e) {
                    $args = $queue->getArguments();

                    $args['headers']['x-attempts']++;

                    $args['headers']['x-exception'] = json_encode([
                        'message' => $e->getMessage(),
                        'code' => $e->getCode(),
                        'trace' => $e->getTrace()
                    ], JSON_UNESCAPED_UNICODE);

                    $queue->setArguments($args);

                    $queue->nack($msg->getDeliveryTag());
                }
            }

            $queue->ack($msg->getDeliveryTag());
        };

        while ($this->channel->isConnected()) {
            /** @var AMQPQueue $queue */
            foreach ($this->queues as $queue) {
                try {
                    $queue->consume($callback);
                } catch (AMQPException $e) {
                    throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
                }
            }

            usleep(1000);
        }

        $this->channel->close();

        try {
            $this->connection->disconnect();
        } catch (Exception $e) {
            throw new \MaxSky\AMQP\Exception\AMQPConnectionException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    protected function prepare() {
        $this->exchange_name = $this->config->connection_name;
        $exchangeName = $this->exchange_name;

        try {
            // declare normal exchange
            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setName($this->exchange_name);
            $this->exchange->setType(AMQPExchangeType::TOPIC);
            $this->exchange->setFlags(AMQP_DURABLE);
            $this->exchange->declare();

            // declare delay exchange
            $delayExchange = new AMQPExchange($this->channel);
            $delayExchange->setName("$this->exchange_name.delay");
            $delayExchange->setType(AMQPExchangeType::DELAYED);
            $delayExchange->setFlags(AMQP_DURABLE);
            $delayExchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
            $delayExchange->declare();

            $retry = $this->options['type'] === 'retry';

            if ($retry) {
                $exchangeName .= '.retry';

                // declare retry exchange
                $exchange = new AMQPExchange($this->channel);
                $exchange->setName($exchangeName);
                $exchange->setType(AMQPExchangeType::TOPIC);
                $exchange->setFlags(AMQP_DURABLE);
                $exchange->declare();
            }

            foreach ($this->options['queues'] as $queue_name) {
                $queue = new AMQPQueue($this->channel);

                $queue->setFlags(AMQP_DURABLE);

                $args = [];

                if ($retry) {
                    $queue_name .= '.retry';

                    if ($this->config->queue_ttl) {
                        $args['x-message-ttl'] = $this->config->queue_ttl;
                    }
                } else {
                    $args['x-dead-letter-exchange'] = "$this->exchange_name.retry";
                }

                $queue->setName($queue_name);
                $queue->setArguments($args);
                $queue->declare();
                $queue->bind($exchangeName);

                $this->queues[] = $queue;
            }
        } catch (AMQPException $e) {
            $this->connection->disconnect();

            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }
}
