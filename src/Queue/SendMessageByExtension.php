<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 14
 * Time: 15:24
 */

namespace MaxSky\AMQP\Queue;

use AMQPChannel;
use AMQPException;
use AMQPExchange;
use AMQPQueue;
use Carbon\Carbon;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPQueueException;

class SendMessageByExtension extends AbstractSendMessage {

    /**
     * @param string      $handler
     * @param mixed       $data
     * @param string|null $queue_name
     * @param bool        $transaction
     *
     * @return void
     * @throws AMQPQueueException
     * @throws \AMQPConnectionException
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', bool $transaction = false) {
        $this->paramsFilter($handler, $data, $queue_name);

        try {
            $this->retryQueue->setName("$queue_name.retry");
            $this->retryQueue->declare();

            $this->queue->setName($queue_name);
            $this->queue->declare();

            $this->queue->bind($this->exchange_name);
        } catch (AMQPException $e) {
            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }

        $message = $this->getAMQPMessage($handler, $data);
        $headers = $this->getAMQPMessageHeader($this->delay_msec);

        if ($transaction) {
            try {
                $this->channel->startTransaction();

                $this->exchange->publish($message, null, null, $headers);

                $this->channel->commitTransaction();
            } catch (AMQPException $e) {
                $channel->rollbackTransaction();

                $this->connection->disconnect();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        } else {
            try {
                $this->exchange->publish($message, null, null, $headers);
            } catch (AMQPException $e) {
                $this->connection->disconnect();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        }

        $this->channel->close();
        $this->connection->disconnect();
    }

    /**
     * @return void
     * @throws AMQPQueueException
     * @throws \AMQPConnectionException
     */
    protected function prepare() {
        try {
            // Create and declare channel
            $this->channel = new AMQPChannel($this->connection);

            // AMQPC Exchange is the publishing mechanism
            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setFlags(AMQP_DURABLE);

            $this->exchange_name = $this->config->connection_name;

            if ($this->delay_msec) {
                $this->exchange_name .= '.delay';

                $this->exchange->setType(AMQPExchangeType::DELAYED);
                $this->exchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
            } else {
                $this->exchange->setType(AMQPExchangeType::TOPIC);
            }

            $this->exchange->setName($this->exchange_name);
            $this->exchange->declare();

            $this->retryQueue = new AMQPQueue($this->channel);
            $this->retryQueue->setFlags(AMQP_DURABLE);

            $this->queue = new AMQPQueue($this->channel);
            $this->queue->setFlags(AMQP_DURABLE);

            $args = [
                'x-dead-letter-exchange' => "$this->exchange_name.retry"
            ];

            if ($this->config->queue_ttl) {
                $args['x-message-ttl'] = $this->config->queue_ttl;
            }

            $this->queue->setArguments($args);
        } catch (AMQPException $e) {
            $this->connection->disconnect();

            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }

    /**
     * @param string $handler
     * @param mixed  $data
     *
     * @return false|string
     */
    private function getAMQPMessage(string $handler, $data) {
        return json_encode([
            'handler' => $handler,
            'data' => $data
        ], JSON_UNESCAPED_UNICODE);
    }

    /**
     * @param int   $delay
     * @param array $extra_headers
     *
     * @return array
     */
    private function getAMQPMessageHeader(int $delay = 0, array $extra_headers = []): array {
        return [
            'delivery_mode' => AMQP_DELIVERY_MODE_PERSISTENT, // write to disk
            'content_type' => 'application/json',
            'timestamp' => Carbon::now()->timestamp,
            'headers' => array_merge([
                'x-delay' => $delay,
                'x-attempts' => 0,
                'x-exception' => null
            ], $extra_headers)
        ];
    }
}
