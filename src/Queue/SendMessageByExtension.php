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
use DateTimeInterface;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;

class SendMessageByExtension extends AbstractSendMessage {

    /**
     * @param string                     $handler
     * @param mixed                      $data
     * @param string|null                $queue_name
     * @param int|DateTimeInterface|null $delay
     * @param bool                       $transaction
     *
     * @return void
     * @throws AMQPQueueException
     * @throws \AMQPConnectionException
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', $delay = null, bool $transaction = false) {
        $this->paramsFilter($handler, $data, $queue_name, $delay);

        $exchangeName = $this->config->connection_name;

        if ($delay) {
            $exchangeName .= '.delay';

            $this->exchange->setType(AMQPExchangeType::DELAYED);
            $this->exchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
        } else {
            $this->exchange->setType(AMQPExchangeType::TOPIC);
        }

        $this->exchange->setName($exchangeName);

        try {
            $this->exchange->declare();

            $this->retryQueue->setName("$queue_name.retry");
            $this->retryQueue->declare();

            $this->queue->setName($queue_name);
            $this->queue->setArgument('x-dead-letter-exchange', "$exchangeName.retry");
            $this->queue->declare();

            $this->queue->bind($exchangeName);
        } catch (AMQPException $e) {
            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }

        $message = $this->getAMQPMessage($handler, $data);
        $headers = $this->getAMQPMessageHeader($delay);

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
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    protected function prepare() {
        try {
            // Create and declare channel
            $this->channel = new AMQPChannel($this->connection);

            // AMQPC Exchange is the publishing mechanism
            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setFlags(AMQP_DURABLE);

            $this->retryQueue = new AMQPQueue($this->channel);
            $this->retryQueue->setFlags(AMQP_DURABLE);

            $this->queue = new AMQPQueue($this->channel);
            $this->queue->setArguments([
                // 'x-dead-letter-routing-key' => 'dead_letter_routing_key',
                'x-message-ttl' => $this->config->queue_ttl
            ]);
        } catch (AMQPException $e) {
            try {
                $this->connection->disconnect();
            } catch (\AMQPConnectionException $e) {
                throw new AMQPConnectionException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }

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
     * @param int|null $delay
     * @param array    $extra_headers
     *
     * @return array
     */
    private function getAMQPMessageHeader(?int $delay = 0, array $extra_headers = []): array {
        return [
            'delivery_mode' => AMQP_DELIVERY_MODE_PERSISTENT, // write to disk
            'content_type' => 'application/json',
            'timestamp' => Carbon::now()->timestamp,
            'headers' => array_merge([
                'x-delay' => $delay * 1000,
                'x-attempts' => 0,
                'x-exception' => null
            ], $extra_headers)
        ];
    }
}
