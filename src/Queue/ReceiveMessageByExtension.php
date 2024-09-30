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
        try {
            $this->channel = new AMQPChannel($this->connection);

            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setFlags(AMQP_DURABLE);

            $this->exchange_name = $this->config->connection_name;

            $exchange = new AMQPExchange($this->channel);
            $exchange->setFlags(AMQP_DURABLE);
            $exchange->setType(AMQPExchangeType::TOPIC);
            $exchange->setName($this->exchange_name);
            $exchange->declare();

            $retry = $this->options['type'] === 'retry';

            if ($retry) {
                $this->exchange_name .= '.retry';
            } else if ($this->options['delay']) {
                $this->exchange_name .= '.delay';

                $this->exchange->setType(AMQPExchangeType::DELAYED);
                $this->exchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
            } else {
                $this->exchange->setType(AMQPExchangeType::TOPIC);
            }

            $this->exchange->setName($this->exchange_name);
            $this->exchange->declare();

            foreach ($this->options['queues'] as $queue_name) {
                $queue = new AMQPQueue($this->channel);

                if ($retry) {
                    $queue_name .= '.retry';

                    $args = [
                        'x-dead-letter-exchange' => "$this->exchange_name.retry"
                    ];

                    if ($this->config->queue_ttl) {
                        $args['x-message-ttl'] = $this->config->queue_ttl;
                    }

                    $queue->setArguments($args);
                }

                $queue->setName($queue_name);
                $queue->setFlags(AMQP_DURABLE);
                $queue->declare();
                $queue->bind($this->exchange_name);

                $this->queues[] = $queue;
            }
        } catch (AMQPException $e) {
            $this->connection->disconnect();

            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }
}
