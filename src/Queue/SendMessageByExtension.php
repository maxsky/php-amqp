<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 14
 * Time: 15:24
 */

namespace MaxSky\AMQP\Queue;

use AMQPChannelException;
use AMQPException;
use AMQPExchange;
use AMQPExchangeException;
use AMQPQueue;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Exception\AMQPRuntimeException;

class SendMessageByExtension extends AbstractSendMessage {

    /**
     * @param string      $handler
     * @param mixed       $data
     * @param string|null $queue_name
     * @param bool        $transaction
     *
     * @return void
     * @throws AMQPQueueException
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', bool $transaction = false) {
        $this->paramsFilter($handler, $data, $queue_name);

        try {
            $this->retryQueue->setName("$queue_name.retry");
            $this->retryQueue->declare();
            $this->retryQueue->bind("$this->exchange_name.retry", "$queue_name.retry");

            $this->queue->setName($queue_name);
            $this->queue->declare();

            if ($this->delay_msec) {
                $this->queue->bind("$this->exchange_name.delay");
            } else {
                $this->queue->bind($this->exchange_name, $queue_name);
            }
        } catch (AMQPException $e) {
            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e);
        }

        $message = $this->getAMQPMessage($handler, $data);
        $headers = $this->getAMQPMessageHeader($this->delay_msec);

        if ($transaction) {
            try {
                $this->channel->startTransaction();

                $this->exchange->publish($message, $queue_name, null, $headers);

                $this->channel->commitTransaction();
            } catch (AMQPException $e) {
                $channel->rollbackTransaction();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e);
            }
        } else {
            try {
                $this->exchange->publish($message, $queue_name, null, $headers);
            } catch (AMQPException $e) {
                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     * @throws AMQPRuntimeException
     */
    protected function prepare() {
        $this->exchange_name = $this->config->connection_name;

        try {
            $retryExchange = new AMQPExchange($this->channel);
            $retryExchange->setName("$this->exchange_name.retry");
            $retryExchange->setType(AMQPExchangeType::TOPIC);
            $retryExchange->setFlags(AMQP_DURABLE);
            $retryExchange->declare();

            $this->exchange = new AMQPExchange($this->channel);
            $this->exchange->setFlags(AMQP_DURABLE);

            if ($this->delay_msec) {
                $this->exchange->setName("$this->exchange_name.delay");

                $this->exchange->setType(AMQPExchangeType::DELAYED);
                $this->exchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
            } else {
                $this->exchange->setName($this->exchange_name);
                $this->exchange->setType(AMQPExchangeType::TOPIC);
            }

            $this->exchange->declare();

            $this->retryQueue = new AMQPQueue($this->channel);
            $this->retryQueue->setFlags(AMQP_DURABLE);

            $this->queue = new AMQPQueue($this->channel);
            $this->queue->setFlags(AMQP_DURABLE);

            if ($this->config->queue_ttl) {
                $this->retryQueue->setArgument('x-message-ttl', $this->config->queue_ttl);
            }

            $this->queue->setArgument('x-dead-letter-exchange', "$this->exchange_name.retry");
        } catch (AMQPChannelException|AMQPExchangeException $e) {
            throw new AMQPRuntimeException($e->getMessage(), $e->getCode(), $e);
        } catch (\AMQPQueueException $e) {
            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e);
        } catch (\AMQPConnectionException $e) {
            throw new AMQPConnectionException($e->getMessage(), $e->getCode(), $e);
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
     * @param int   $delay 延迟毫秒
     * @param array $extra_headers
     *
     * @return array
     */
    private function getAMQPMessageHeader(int $delay = 0, array $extra_headers = []): array {
        return [
            'delivery_mode' => AMQP_DELIVERY_MODE_PERSISTENT, // write to disk
            'content_type' => 'application/json',
            'timestamp' => time(),
            'headers' => array_merge([
                'x-delay' => $delay,
                'x-attempts' => 0,
                'x-exception' => ''
            ], $extra_headers)
        ];
    }
}
