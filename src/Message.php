<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 17:40
 */

namespace MaxSky\AMQP;

use AMQPConnection;
use DateTimeInterface;
use MaxSky\AMQP\Config\AMQPBaseConnection;
use MaxSky\AMQP\Config\AMQPConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Queue\AbstractSendMessage;
use MaxSky\AMQP\Queue\SendMessage;
use MaxSky\AMQP\Queue\SendMessageByExtension;
use PhpAmqpLib\Connection\AbstractConnection;

class Message {

    private static $instance;

    private $config;

    /** @var AMQPConnection|AbstractConnection */
    private $connection;

    /** @var AbstractSendMessage */
    private $messageService;

    /**
     * @param AMQPConfig $config
     *
     * @throws AMQPConnectionException
     */
    public function __construct(AMQPConfig $config) {
        $this->config = $config;

        $this->connection = (new AMQPBaseConnection($config))->getConnection();

        $this->declare();
    }

    /**
     * @param AMQPConfig $config
     *
     * @return Message
     * @throws AMQPConnectionException
     */
    public static function init(AMQPConfig $config): Message {
        if (!self::$instance) {
            self::$instance = new self($config);
        }

        return self::$instance;
    }

    /**
     * @param string                     $handler
     * @param mixed                      $data
     * @param string|null                $queue_name
     * @param int|DateTimeInterface|null $delay
     * @param bool                       $transaction
     *
     * @return void
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', $delay = null, bool $transaction = false) {
        $this->messageService->send($handler, $data, $queue_name, $delay, $transaction);
    }

    /**
     * @return void
     * @throws AMQPConnectionException
     */
    private function declare() {
        if (extension_loaded('amqp')) {
            $this->messageService = new SendMessageByExtension($this->connection, $this->config->connection_name);
        } else {
            $this->messageService = new SendMessage($this->connection, $this->config->connection_name);
        }
    }
}
