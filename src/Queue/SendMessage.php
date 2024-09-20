<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 17:39
 */

namespace MaxSky\AMQP\Queue;

use Carbon\Carbon;
use Exception;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class SendMessage extends AbstractSendMessage {

    /**
     * 发送消息队列
     *
     * @param string      $handler     队列处理器
     * @param mixed       $data        队列数据
     * @param string|null $queue_name  队列名
     * @param bool        $transaction 是否开启事务
     *
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', bool $transaction = false) {
        $this->paramsFilter($handler, $data, $queue_name);

        $this->channel->queue_declare(
            "$queue_name.retry", false, true, false, false
        );

        $this->channel->queue_declare(
            $queue_name, false, true, false, false, false, new AMQPTable([
                'x-dead-letter-exchange' => "$this->exchange_name.retry"
                // 'x-dead-letter-routing-key' => 'dead_letter_routing_key',
                // 'x-message-ttl' => $this->config->queue_ttl
            ])
        );

        $this->channel->queue_bind($queue_name, $this->exchange_name);

        $message = $this->getAMQPMessage($handler, $data, $this->delay_msec);

        if ($transaction) {
            $this->channel->tx_select();

            try {
                $this->channel->basic_publish($message, $this->exchange_name);

                $this->channel->tx_commit();
            } catch (AMQPRuntimeException $e) {
                $this->channel->tx_rollback();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        } else {
            try {
                $this->channel->basic_publish($message, $this->exchange_name);
            } catch (AMQPRuntimeException $e) {
                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        }

        try {
            $this->channel->close();
            $this->connection->close();
        } catch (Exception $e) {
            throw new AMQPConnectionException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }

    protected function prepare() {
        $this->channel = $this->connection->channel();

        $this->exchange_name = $this->config->connection_name;

        if ($this->delay_msec) {
            $this->exchange_name .= '.delay';

            // declare normal delay exchange
            $this->channel->exchange_declare($this->exchange_name, AMQPExchangeType::DELAYED,
                false, true, false, false, false, new AMQPTable([
                    'x-delayed-type' => AMQPExchangeType::TOPIC
                ])
            );
        } else {
            // declare normal exchange
            $this->channel->exchange_declare(
                $this->exchange_name, AMQPExchangeType::TOPIC, false, true, false
            );
        }
    }

    /**
     * @param string $handler 队列处理器
     * @param mixed  $data    队列数据
     * @param int    $delay   延迟时间，毫秒
     *
     * @return AMQPMessage
     */
    private function getAMQPMessage(string $handler, $data, int $delay = 0): AMQPMessage {
        return new AMQPMessage(json_encode([
            'handler' => $handler,
            'data' => $data
        ], JSON_UNESCAPED_UNICODE), [
            'application_headers' => new AMQPTable([
                'x-delay' => $delay,
                'x-attempts' => 0,
                'x-exception' => null
            ]),
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT, // write to disk
            'content_type' => 'application/json',
            'timestamp' => Carbon::now()->timestamp,
            // message alive time, the message will discard when time up, must be a string
            //'expiration' => (string)(($expiration ?: 60) * 1000),
        ]);
    }
}
