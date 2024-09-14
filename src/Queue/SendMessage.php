<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 17:39
 */

namespace MaxSky\AMQP\Queue;

use Carbon\Carbon;
use DateTimeInterface;
use Exception;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPQueueException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class SendMessage extends AbstractSendMessage {

    /**
     * 发送消息队列
     *
     * @param string                     $handler     队列处理器
     * @param mixed                      $data        队列数据
     * @param string|null                $queue_name  队列名
     * @param int|DateTimeInterface|null $delay       延迟秒
     * @param bool                       $transaction 是否开启事务
     *
     * @return void
     * @throws AMQPQueueException
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', $delay = null, bool $transaction = false) {
        $this->paramsFilter($handler, $data, $delay);

        /** @var AbstractConnection $connection */
        $channel = $this->connection->channel();

        $exchangeName = $this->connectionName;

        if ($delay) {
            $exchangeName .= '.delay';

            // declare normal delay exchange
            $channel->exchange_declare($exchangeName, AMQPExchangeType::DELAYED,
                false, true, false, false, false, new AMQPTable([
                    'x-delayed-type' => AMQPExchangeType::TOPIC
                ])
            );
        } else {
            // declare normal exchange
            $channel->exchange_declare(
                $exchangeName, AMQPExchangeType::TOPIC, false, true, false
            );
        }

        // declare retry exchange, the another one exchange for failed can declare if you need
        $channel->exchange_declare("$exchangeName.retry", AMQPExchangeType::TOPIC,
            false, true, false);

        $queueName = $queue_name ?: 'default';

        // declare queue, always a normal queue at this
        $channel->queue_declare($queueName, false, true, false, false, false,
            new AMQPTable([
                'x-queue-mode' => 'lazy'
            ])
        );

        // bind queue to exchange and use queue name as routing key
        // $channel->queue_bind($queueName, $exchangeName, $queueName);

        if ($transaction) {
            $channel->tx_select();

            try {
                $channel->basic_publish($this->getAMQPMessage($handler, $data, $delay), $exchangeName, $queueName);

                $channel->tx_commit();
            } catch (AMQPRuntimeException $e) {
                $channel->tx_rollback();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        } else {
            try {
                $channel->basic_publish($this->getAMQPMessage($handler, $data, $delay), $exchangeName, $queueName);
            } catch (AMQPRuntimeException $e) {
                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        }

        $channel->close();

        try {
            $this->connection->close();
        } catch (Exception $e) {
            throw new $e;
        }
    }

    /**
     * @param string   $handler 队列处理器
     * @param mixed    $data    队列数据
     * @param int|null $delay   延迟时间
     *
     * @return AMQPMessage
     */
    private function getAMQPMessage(string $handler, $data, ?int $delay = 0): AMQPMessage {
        $payload = [
            'handler' => $handler,
            'data' => $data,
            'create_time' => Carbon::now()->toDateTimeString()
        ];

        return new AMQPMessage(json_encode($payload, JSON_UNESCAPED_UNICODE), [
            'application_headers' => new AMQPTable([
                'x-delay' => $delay * 1000,
                'x-attempts' => 1,
                'x-exception' => null
            ]),
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT, // write to disk
            // message alive time, the message will discard when time up, must be a string
            //'expiration' => (string)(($expiration ?: 60) * 1000),
        ]);
    }
}
