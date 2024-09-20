<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 14
 * Time: 15:34
 */

namespace MaxSky\AMQP\Command;

use AMQPConnection;
use Exception;
use Illuminate\Console\Command;
use MaxSky\AMQP\Config\AMQPBaseConnection;
use MaxSky\AMQP\Config\AMQPConfig;
use MaxSky\AMQP\Config\AMQPExchangeType;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

if (!class_exists(Command::class)) {
    throw new Exception('Laravel Command class not found');
}

class AMQPReceiveLaravelCommand extends Command {

    use AMQPReceiveByExtensionTrait;

    protected $name = 'amqp';
    protected $signature = 'amqp:receive
                            {receive          : Use this argument to receive messages}
                            {--d|delay        : Use this option to receive delayed messages, default receive instant messages}
                            {--queues=default : Set the queue name, multiple queues are separated by ","}
                            {--type=normal    : Set queue type, optional retry}
                            {--ttl=60         : Set a number of seconds for child process can run, change this option may delete exist queue}
                            {--tries=3        : Set a number of attempt for a queue}';
    protected $description = 'Receive and handle AMQP messages';

    private $config;
    /** @var AMQPConnection|AbstractConnection */
    private $connection;

    private $delay;
    /** @var string[] */
    private $queues;
    private $type;
    private $ttl;
    private $tries;
    private $serializer = 'json';

    /**
     * @throws AMQPConnectionException
     */
    public function __construct() {
        parent::__construct();

        $this->config = new AMQPConfig();

        $this->config->connection_name = config('amqp.connection.name');
        $this->config->host = config('amqp.connection.host');
        $this->config->port = config('amqp.connection.port');
        $this->config->user = config('amqp.connection.user');
        $this->config->password = config('amqp.connection.password');
        $this->config->vhost = config('amqp.connection.vhost');

        $this->config->queue_ttl = config('amqp.arguments.queue_ttl');

        $this->config->connect_options = config('amqp.connection.options');

        $this->initCommandOptions();

        $this->connection = (new AMQPBaseConnection($this->config))->getConnection();
    }

    /**
     * 消息处理
     *
     * @return void
     * @throws Exception
     */
    public function handle(): void {
        $channel = $connection->channel();

        $this->declareHandler($channel);

        $this->info("[{$this->getDateTimeString()}] Waiting for $this->type messages. To exit press control+C");

        // $channel->basic_qos(0, 1, null);

        /** @var SerializerFactoryInterface $factory */
        $factory = SerializerFactory::{$this->serializer}();

        foreach ($this->queues as $queue) {
            $channel->basic_consume("$queue.$this->type", '', false, true,
                false, false, function (AMQPMessage $msg) use ($queue, $factory) {
                    // get headers data
                    /** @var AMQPTable $table */
                    $table = $msg->get('application_headers');

                    $headers = $table->getNativeData();

                    // get body data
                    /** @var array $body */
                    $body = $factory->unserialize($msg->getBody());

                    if ($headers['x-exception'] || $headers['x-attempts'] > $this->tries) {
                        $this->queueFailedHandle($body['handler'], $queue, $body['data'], $headers['x-exception']);
                    } else {
                        try {
                            $this->queueHandle($body['handler'], $body['data'], $result);

                            if ($result === false) {
                                $this->queueFailedHandle(
                                    $body['handler'], $queue, $body['data'], $headers['x-exception']
                                );
                            }

                            $this->info("[{$this->getDateTimeString()}] Queue \"{$body['handler']}\" handled.");
                        } catch (Exception $e) {
                            Log::channel('job')
                                ->error('RabbitMQ Handle error: ' . $e->getMessage(), $e->getTrace());

                            $this->warn("[{$this->getDateTimeString()}] Queue \"{$body['handler']}\" retried.");

                            $table->set('x-attempts', ++$headers['x-attempts']);

                            // cannot use array as value in header
                            $table->set('x-exception', json_encode([
                                'message' => $e->getMessage(),
                                'code' => $e->getCode(),
                                'trace' => $e->getTrace()
                            ]));

                            $msg->set('application_headers', $table);

                            $msg->getChannel()->basic_publish($msg, EXCHANGE_RETRY, "$queue.retry");
                        }
                    }
                }
            );
        }

        while (count($channel->callbacks)) {
            $channel->wait();
        }

        $channel->close();
        $connection->close();
    }

    /**
     * 初始化命令选项
     *
     * @return void
     */
    private function initCommandOptions(): void {
        $this->type = strtolower($this->option('type'));

        if (!in_array($this->type, ['normal', 'retry'])) {
            exit('Queue type invalid.');
        }

        $this->delay = (bool)$this->option('delay');

        if ($this->delay) {
            define('EXCHANGE_NORMAL', $this->config->connection_name . '.delay');
            define('EXCHANGE_RETRY', $this->config->connection_name . '.delay.retry');

        } else {
            define('EXCHANGE_NORMAL', $this->config->connection_name);
            define('EXCHANGE_RETRY', $this->config->connection_name . '.retry');
        }

        define('EXCHANGE_CURRENT', $this->type === 'normal' ? constant('EXCHANGE_NORMAL') : constant('EXCHANGE_RETRY'));

        $queues = array_filter(explode(',', $this->option('queues')));

        if (!$queues) {
            $queues = ['default'];
        }

        $this->queues = $queues;

        $ttl = (int)$this->option('ttl');

        if (!$ttl) {
            $ttl = 60;
        }

        $this->ttl = $ttl;

        $tries = (int)$this->option('tries');

        if ($tries < 0) {
            $tries = 0;
        }

        $this->tries = $tries;
    }

    /**
     * @param AMQPChannel $channel
     *
     * @return void
     */
    private function declareHandler(AMQPChannel $channel): void {
        if ($this->delay && $this->type === 'normal') {
            // declare normal delay exchange
            $channel->exchange_declare(
                EXCHANGE_CURRENT,
                AMQPExchangeType::DELAYED,
                false,
                true,
                false,
                false,
                false,
                new AMQPTable([
                    'x-delayed-type' => AMQPExchangeType::TOPIC
                ])
            );
        } else {
            // declare normal exchange
            $channel->exchange_declare(
                EXCHANGE_CURRENT,
                AMQPExchangeType::TOPIC,
                false,
                true,
                false
            );
        }

        $this->queueDeclare($channel);
    }

    /**
     * @param AMQPChannel $channel
     *
     * @return void
     */
    private function queueDeclare(AMQPChannel $channel): void {
        // default use lazy mode
        $args = [
            'x-queue-mode' => 'lazy'
        ];

        if ($this->type === 'retry') {
            $args = array_merge($args, [
                'x-dead-letter-exchange' => EXCHANGE_NORMAL,
                'x-message-ttl' => $this->ttl * 1000
            ]);
        }

        foreach ($this->queues as $queue) {
            $queue = "$queue.$this->type";

            $channel->queue_declare($queue,
                false, true, false, false, false, new AMQPTable($args));

            $channel->queue_bind($queue, EXCHANGE_CURRENT, $queue);
        }
    }

    /**
     * @return string
     */
    private function getDateTimeString(): string {
        return Carbon::now()->toDateTimeString();
    }

    /**
     * 失败处理
     *
     * @param string      $class
     * @param string      $queue
     * @param mixed       $data
     * @param string|null $exception
     *
     * @return void
     */
    private function queueFailedHandle(string $class, string $queue, $data, ?string $exception = null): void {
        /** @var Handler $handler */
        $handler = app($class);

        $handler->failed([
            'connection' => 'amqp',
            'queue' => $queue,
            'payload' => json_encode($data),
            'exception' => $exception
        ]);

        $this->info("[{$this->getDateTimeString()}] Queue \"$class\" failed handle complete.");
    }

    /**
     * 队列处理
     *
     * @param string $class  目标类
     * @param mixed  $data   队列数据
     * @param mixed  $result 返回结果
     *
     * @return void
     * @throws RabbitMQHandlerException
     */
    private function queueHandle(string $class, $data, &$result = null): void {
        /** @var Handler $handler */
        $handler = app($class);

        $this->info("[{$this->getDateTimeString()}] Queue \"$class\" Handing.");

        $handler->handle($data, $result);
    }
}
