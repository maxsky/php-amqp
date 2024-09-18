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



        $message = $this->getAMQPMessage($handler, $data);
        $headers = $this->getAMQPMessageHeader($delay);

        if ($transaction) {
            try {
                $channel->startTransaction();

                $exchange->publish($message, null, null, $headers);

                $channel->commitTransaction();
            } catch (AMQPException $e) {
                $channel->rollbackTransaction();

                $this->connection->disconnect();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        } else {
            try {
                $exchange->publish($message, null, null, $headers);
            } catch (AMQPException $e) {
                $this->connection->disconnect();

                throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
            }
        }

        $channel->close();
        $this->connection->disconnect();
    }

    protected function declare() {
        try {
            // Create and declare channel
            $this->channel = new AMQPChannel($this->connection);

            // AMQPC Exchange is the publishing mechanism
            $exchange = new AMQPExchange($this->channel);
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
            $exchange->declare();

            $queue = new AMQPQueue($channel);

            $queueName = $queue_name ?: 'default';

            $queue->setName($queueName);
            $queue->setFlags(AMQP_DURABLE);
            $queue->declare();

            $queue->bind($exchangeName);
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
