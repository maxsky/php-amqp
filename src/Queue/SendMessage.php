<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 17:39
 */

namespace MaxSky\AMQP\Queue;

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

        $args = null;

        if ($this->config->queue_ttl) {
            $args = [
                'x-message-ttl' => $this->config->queue_ttl
            ];
        }

        $this->channel->queue_declare(
            "$queue_name.retry", false, true, false, false, false, new AMQPTable($args)
        );

        $this->channel->queue_bind("$queue_name.retry", "$this->exchange_name.retry", "$queue_name.retry");

        $args = [
            'x-dead-letter-exchange' => "$this->exchange_name.retry"
        ];

        $this->channel->queue_declare(
            $queue_name, false, true, false, false, false, new AMQPTable($args)
        );

        $this->channel->queue_bind($queue_name, $this->exchange_name, $queue_name);

        $message = $this->getAMQPMessage($handler, $data, $this->delay_msec);

        if ($transaction) {
            $this->channel->tx_select();

            try {
                $this->channel->basic_publish($message, $this->exchange_name, $queue_name);

                $this->channel->tx_commit();
            } catch (AMQPRuntimeException $e) {
                $this->channel->tx_rollback();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        } else {
            $exchangeName = $this->delay_msec ? "$this->exchange_name.delay" : $this->exchange_name;

            try {
                $this->channel->basic_publish($message, $exchangeName, $queue_name);
            } catch (AMQPRuntimeException $e) {
                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        }
    }

    protected function prepare() {
        $this->exchange_name = $this->config->connection_name;

        if ($this->delay_msec) {
            // declare normal delay exchange
            $this->channel->exchange_declare("$this->exchange_name.delay", AMQPExchangeType::DELAYED,
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

        $this->channel->exchange_declare(
            "$this->exchange_name.retry", AMQPExchangeType::TOPIC, false, true, false
        );
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
                'x-exception' => ''
            ]),
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT, // write to disk
            'content_type' => 'application/json',
            'timestamp' => time()
        ]);
    }
}
