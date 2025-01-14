<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 24
 * Time: 11:05
 */

namespace MaxSky\AMQP\Queue;

use Exception;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use RuntimeException;

class ReceiveMessage extends AbstractReceiveMessage {

    /**
     * @return void
     * @throws AMQPConnectionException
     */
    public function receive() {
        $retry = $this->options['type'] === 'retry';

        foreach ($this->options['queues'] as $queue_name) {
            if ($retry) {
                $queue_name .= '.retry';
            }

            $this->channel->basic_consume(
                $queue_name,
                '',
                false,
                false,
                false,
                false,
                function (AMQPMessage $msg) use ($queue_name) {
                    // get headers data
                    /** @var AMQPTable $headerTable */
                    $headerTable = $msg->get('application_headers');

                    $headers = $headerTable->getNativeData();

                    // get body data
                    /** @var array $body */
                    $body = json_decode($msg->getBody(), true);

                    if ($headers['x-exception'] || $headers['x-attempts'] > $this->options['tries']) {
                        $body['timestamp'] = $msg->get('timestamp');

                        $this->failedHandle($body['handler'], $queue_name, $body, $headers);
                    } else {
                        try {
                            $this->queueHandle($body['handler'], $body['data'], $result);

                            if ($result === false) {
                                $this->failedHandle($body['handler'], $queue_name, $body['data'], $headers);
                            }
                        } catch (Exception $e) {
                            $headerTable->set('x-attempts', ++$headers['x-attempts']);

                            // cannot use array as value in header
                            $headerTable->set('x-exception', json_encode([
                                'message' => $e->getMessage(),
                                'code' => $e->getCode(),
                                'trace' => $e->getTrace()
                            ], JSON_UNESCAPED_UNICODE));

                            $msg->set('application_headers', $headerTable);
                            // message alive time, the message will discard when time up, must be a string
                            //'expiration' => (string)(($expiration ?: 60) * 1000),

                            $this->channel->basic_nack($msg->getDeliveryTag());
                        }
                    }

                    $this->channel->basic_ack($msg->getDeliveryTag());
                }
            );
        }

        while ($this->channel->is_consuming()) {
            $this->channel->wait();
        }

        try {
            $this->channel->close();
            $this->connection->close();
        } catch (Exception $e) {
            throw new AMQPConnectionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @return void
     * @throws AMQPQueueException
     * @throws AMQPRuntimeException
     */
    protected function prepare() {
        $this->exchange_name = $this->config->connection_name;
        $exchangeName = $this->exchange_name;

        $retry = $this->options['type'] === 'retry';

        try {
            // declare normal exchange
            $this->channel->exchange_declare(
                $this->exchange_name,
                AMQPExchangeType::TOPIC,
                false,
                true,
                false
            );

            // declare delay exchange
            $this->channel->exchange_declare(
                "$this->exchange_name.delay",
                AMQPExchangeType::DELAYED,
                false,
                true,
                false,
                false,
                false,
                new AMQPTable([
                    'x-delayed-type' => AMQPExchangeType::TOPIC
                ])
            );

            if ($retry) {
                $exchangeName .= '.retry';

                // declare retry exchange
                $this->channel->exchange_declare(
                    $exchangeName,
                    AMQPExchangeType::TOPIC,
                    false,
                    true,
                    false
                );
            }
        } catch (RuntimeException $e) {
            throw new AMQPRuntimeException($e->getMessage(), $e->getCode(), $e);
        }

        foreach ($this->options['queues'] as $queue_name) {
            $args = [];

            if ($retry) {
                $queue_name .= '.retry';

                if ($this->config->queue_ttl) {
                    $args['x-message-ttl'] = $this->config->queue_ttl;
                }
            } else {
                $args['x-dead-letter-exchange'] = "$this->exchange_name.retry";
            }

            try {
                $args = new AMQPTable($args);

                $this->channel->queue_declare(
                    $queue_name, false, true, false, false, false, $args
                );

                $this->channel->queue_bind($queue_name, $exchangeName);
            } catch (RuntimeException $e) {
                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }
}
