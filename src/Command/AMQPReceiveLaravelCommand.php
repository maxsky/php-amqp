<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 14
 * Time: 15:34
 */

namespace MaxSky\AMQP\Command;

use Carbon\Carbon;
use Exception;
use Illuminate\Console\Command;
use MaxSky\AMQP\Config\AMQPConfig;
use MaxSky\AMQP\Queue\ReceiveMessage;
use MaxSky\AMQP\Queue\ReceiveMessageByExtension;

if (!class_exists(Command::class)) {
    throw new Exception('Laravel/Lumen Command class not found');
}

class AMQPReceiveLaravelCommand extends Command {

    protected $name = 'amqp';
    protected $signature = 'amqp:receive
                            {--d|delay        : Use this option to receive delayed messages, default receive instant messages}
                            {--queues=default : Set the queue name, multiple queues are separated by ","}
                            {--type=normal    : Set queue type, optional retry}
                            {--ttl=60         : Set a number of seconds for child process can run, change this option may delete exist queue}
                            {--tries=3        : Set a number of attempt for a queue}';
    protected $description = 'Receive and handle AMQP messages';

    private $options = [];

    private $config;

    private $messageService;

    /**
     * 消息处理
     *
     * @return void
     * @throws Exception
     */
    public function handle(): void {
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

        if (extension_loaded('amqp')) {
            $this->messageService = new ReceiveMessageByExtension($this->config, $this->options);
        } else {
            $this->messageService = new ReceiveMessage($this->config, $this->options);
        }

        $this->info("[{$this->getDateTimeString()}] Waiting for {$this->options['type']} messages. To exit press control+C.");

        try {
            $this->messageService->receive();
        } catch (Exception $e) {
            $this->error($e->getMessage());
        }
    }

    /**
     * 初始化命令选项
     *
     * @return void
     */
    private function initCommandOptions(): void {
        $this->options['type'] = strtolower($this->option('type'));

        if (!in_array($this->options['type'], ['normal', 'retry'])) {
            exit('Queue type invalid.');
        }

        $this->options['delay'] = (bool)$this->option('delay');

        if ($this->options['delay'] && ($this->options['type'] === 'retry')) {
            exit('Queue retry type and delay can not be used together.');
        }

        $queues = array_filter(explode(',', $this->option('queues')));

        if (!$queues) {
            $queues = ['default'];
        }

        $this->options['queues'] = $queues;

        $ttl = (int)$this->option('ttl');

        if (!$ttl) {
            $ttl = 60;
        }

        $this->options['ttl'] = $ttl;

        $tries = (int)$this->option('tries');

        if ($tries < 0) {
            $tries = 0;
        }

        $this->options['tries'] = $tries;
    }

    /**
     * @return string
     */
    private function getDateTimeString(): string {
        return Carbon::now()->toDateTimeString();
    }
}
