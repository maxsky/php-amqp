<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 14
 * Time: 15:24
 */

namespace MaxSky\AMQP\Queue;

use AMQPChannel;
use AMQPChannelException;
use AMQPConnectionException;
use AMQPException;
use AMQPExchange;
use AMQPQueue;
use Carbon\Carbon;
use DateTimeInterface;
use MaxSky\AMQP\Config\AMQPExchangeType;
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
     * @throws AMQPChannelException
     * @throws AMQPConnectionException
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', $delay = null, bool $transaction = false) {
        $this->paramsFilter($handler, $data, $delay);

        try {
            // Create and declare channel
            $channel = new AMQPChannel($this->connection);

            // AMQPC Exchange is the publishing mechanism
            $exchange = new AMQPExchange($channel);
            $exchange->setFlags(AMQP_DURABLE);

            $exchangeName = $this->connectionName;

            if ($delay) {
                $exchangeName .= '.delay';

                $exchange->setType(AMQPExchangeType::DELAYED);
                $exchange->setArgument('x-delayed-type', AMQPExchangeType::TOPIC);
            } else {
                $exchange->setType(AMQPExchangeType::TOPIC);
            }

            $exchange->setName($exchangeName);

            $retryExchange = new AMQPExchange($channel);
            $retryExchange->setName($exchangeName . '.retry');
            $retryExchange->setType(AMQPExchangeType::TOPIC);
            $retryExchange->setFlags(AMQP_DURABLE);

            $queue = new AMQPQueue($channel);

            $queueName = $queue_name ?: 'default';

            $queue->setName($queueName);
            $queue->setFlags(AMQP_DURABLE);
            $queue->bind($exchangeName, $queueName);
        } catch (AMQPException $e) {
            $this->connection->disconnect();

            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }

        if ($transaction) {
            try {
                $channel->startTransaction();

                $exchange->publish(
                    $this->getAMQPMessage($handler, $data),
                    $queueName,
                    null,
                    $this->getAMQPMessageHeader($delay)
                );

                $channel->commitTransaction();
            } catch (AMQPException $e) {
                $channel->rollbackTransaction();

                $this->connection->disconnect();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        } else {
            try {
                $exchange->publish(
                    $this->getAMQPMessage($handler, $data),
                    $queueName,
                    null,
                    $this->getAMQPMessageHeader($delay)
                );
            } catch (AMQPException $e) {
                $this->connection->disconnect();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        }

        $channel->close();
        $this->connection->disconnect();
    }

    /**
     * @param string $handler
     * @param mixed  $data
     *
     * @return false|string
     */
    private function getAMQPMessage(string $handler, $data) {
        $payload = [
            'handler' => $handler,
            'data' => $data,
            'create_time' => Carbon::now()->toDateTimeString()
        ];

        return json_encode($payload, JSON_UNESCAPED_UNICODE);
    }

    /**
     * @param int|null $delay
     * @param array    $extra_headers
     *
     * @return array
     */
    private function getAMQPMessageHeader(?int $delay = 0, array $extra_headers = []): array {
        return [
            'delivery_mode' => AMQP_DELIVERY_MODE_PERSISTENT, // write to disk, default keep data in RAM
            'headers' => array_merge([
                'x-delay' => $delay * 1000,
                'x-attempts' => 1,
                'x-exception' => null
            ], $extra_headers)
        ];
    }
}
