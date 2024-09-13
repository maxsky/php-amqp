<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 17:39
 */

namespace MaxSky\AMQP\Queue;

use AMQPChannel;
use AMQPConnection;
use AMQPException;
use AMQPExchange;
use AMQPQueue;
use Exception;
use MaxSky\AMQP\Exception\AMQPQueueException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class SendMessage {

    /**
     * @param AMQPConnection $connection
     * @param                $message
     *
     * @return void
     */
    public function sendByExt(AMQPConnection $connection, $message, $queue, bool $delay = false) {
        try {
            // Create and declare channel
            $channel = new AMQPChannel($connection);

            // AMQPC Exchange is the publishing mechanism
            $exchange = new AMQPExchange($channel);

            $exchange->setType(AMQP_EX_TYPE_DIRECT);

            $routing_key = 'hello';

            $queue = new AMQPQueue($channel);
            $queue->setName($routing_key);
            $queue->setFlags(AMQP_NOPARAM);
            $queue->declareQueue();

            $exchange->publish($message, $routing_key);

            $connection->disconnect();
        } catch (AMQPException $e) {

        }
    }

    /**
     * 发送消息队列
     *
     * @param AbstractConnection $connection
     * @param string             $handler     队列处理器
     * @param mixed              $data        队列数据
     * @param string             $queue       对列名
     * @param int|null           $delay       延迟秒
     * @param bool               $transaction 是否开启事务
     *
     * @return bool
     * @throws AMQPQueueException
     */
    public function send(AbstractConnection $connection, string $handler, mixed $data, string $queue = 'default',
                         ?int               $delay = null, bool $transaction = false): bool {
        self::paramsFilter($handler, $data, $queue);

        $channel = $connection->channel();

        $prefix = config('app.shortname');

        if ($delay) {
            $prefix .= '.delay';
            $exchange = "$prefix.normal";

            // declare normal delay exchange
            $channel->exchange_declare($exchange, AMQPExchangeType::DELAYED,
                false, true, false, false, false, new AMQPTable([
                    'x-delayed-type' => AMQPExchangeType::TOPIC
                ])
            );
        } else {
            $exchange = "$prefix.normal";

            // declare normal exchange
            $channel->exchange_declare($exchange, AMQPExchangeType::TOPIC,
                false, true, false);
        }

        // declare retry exchange, the another one exchange for failed can declare if you need
        $channel->exchange_declare("$prefix.retry", AMQPExchangeType::TOPIC,
            false, true, false);

        $queue = "$queue.normal";

        // declare queue, always a normal queue at this
        $channel->queue_declare($queue, false, true, false, false, false,
            new AMQPTable([
                'x-queue-mode' => 'lazy'
            ])
        );

        // bind queue to exchange and use queue name as routing key
        $channel->queue_bind($queue, $exchange, $queue);

        if ($transaction) {
            $channel->tx_select();

            $ret = false;

            try {
                $channel->basic_publish(self::getAMQPMessage($handler, $data, $delay), $exchange, $queue);

                $channel->tx_commit();

                $ret = true;
            } catch (Exception $e) {

                $channel->tx_rollback();

                // return false if throw exception
            }
        } else {
            $channel->basic_publish(self::getAMQPMessage($handler, $data, $delay), $exchange, $queue);

            // not use transaction always return true
            $ret = true;
        }

        try {
            $channel->close();
            $connection->close();
        } catch (Exception $e) {
        }

        return $ret;
    }

    /**
     * 参数过滤
     *
     * @param string $handler 队列处理器
     * @param mixed  $data    队列数据
     * @param string $queue   队列名称
     *
     * @return void
     * @throws AMQPQueueException
     */
    private static function paramsFilter(string $handler, mixed $data, string $queue): void {
        if (!class_exists($handler) || current(class_parents($handler)) !== Handler::class) {
            throw new AMQPQueueException('Handler not exist or not extend from base Handler class.');
        }

        if (!$data) {
            throw new AMQPQueueException('Message could not be empty.');
        }

        if ($channel_id < 1) {
            throw new AMQPQueueException('Channel ID must greater than 0.');
        }

        if (!$queue) {
            throw new AMQPQueueException('Queue name could not be empty.');
        }
    }

    /**
     * 获取 AMQP 消息
     *
     * @param string   $handler 队列处理器
     * @param mixed    $data    队列数据
     * @param int|null $delay   延迟时间
     *
     * @return AMQPMessage
     */
    private static function getAMQPMessage(string $handler, mixed $data, ?int $delay = 0): AMQPMessage {
        $payload = [
            'handler' => $handler,
            'data' => $data,
            'create_time' => Carbon::now()->toDateTimeString()
        ];

        $serializer = config('amqp.config.options.serializer', 'json');

        /** @var SerializerFactoryInterface $factory */
        $factory = SerializerFactory::{$serializer}();

        return new AMQPMessage($factory->serialize($payload), [
            'application_headers' => new AMQPTable([
                'x-delay' => $delay * 1000,
                'x-attempts' => 1,
                'x-exception' => null
            ]),
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            // message alive time, the message will discard when time up, must be a string
            //'expiration' => (string)(($expiration ?: 60) * 1000),
        ]);
    }
}
