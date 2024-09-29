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
use MaxSky\AMQP\Exception\AMQPQueueException;
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
     * @param AMQPConfig|null $config
     *
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    public function __construct(?AMQPConfig $config = null) {
        if (!$config) {
            $config = new AMQPConfig();
        }

        $this->config = $config->getConfig();

        $this->connection = (new AMQPBaseConnection($this->config))->getConnection();

        $this->initService();
    }

    /**
     * @param AMQPConfig|null $config
     *
     * @return Message
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    public static function init(?AMQPConfig $config = null): Message {
        if (!self::$instance) {
            self::$instance = new self($config);
        }

        return self::$instance;
    }

    /**
     * @param int|string|DateTimeInterface $time
     *
     * @return Message
     */
    public function delay($time): Message {
        $this->messageService->delay($time);

        return $this;
    }

    /**
     * @param string      $handler
     * @param mixed       $data
     * @param string|null $queue_name
     * @param bool        $transaction
     *
     * @return void
     */
    public function send(string $handler, $data, ?string $queue_name = 'default', bool $transaction = false) {
        $this->messageService->send($handler, $data, $queue_name, $transaction);
    }

    /**
     * @return void
     * @throws AMQPConnectionException
     * @throws AMQPQueueException
     */
    private function initService() {
        if (extension_loaded('amqp')) {
            $this->messageService = new SendMessageByExtension($this->connection, $this->config);
        } else {
            $this->messageService = new SendMessage($this->connection, $this->config);
        }
    }
}
