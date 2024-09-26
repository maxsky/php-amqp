<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 18
 * Time: 10:33
 */

namespace Tests\Connection;

use AMQPConnection;
use MaxSky\AMQP\Config\AMQPBaseConnection;
use MaxSky\AMQP\Config\AMQPBaseConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use PhpAmqpLib\Connection\AbstractConnection;
use Tests\TestCase;

class TestConnectServer extends TestCase {

    /** @var AMQPBaseConfig */
    private $config;

    protected function setUp(): void {
        parent::setUp();

        $this->config = new AMQPBaseConfig();

        $this->config->connection_name = 'phpunit-test';

        $this->config->host = '127.0.0.1';
        $this->config->port = 5672;
        $this->config->user = 'guest';
        $this->config->password = 'guest';
        $this->config->vhost = '/';

        $this->config->connect_options = [];
    }

    public function testConnectThroughExtension() {
        if (!extension_loaded('amqp')) {
            $this->fail('AMQP Extension not loaded.');
        }

        try {
            $baseConnection = new AMQPBaseConnection($this->config);

            $this->assertInstanceOf(AMQPConnection::class, $baseConnection->getConnection());
        } catch (AMQPConnectionException $e) {
            $this->fail($e->getMessage());
        }
    }

    public function testConnectThoughLibrary() {
        if (extension_loaded('amqp')) {
            $this->fail('AMQP Extension loaded. Please disable.');
        }

        try {
            $baseConnection = new AMQPBaseConnection($this->config);

            $this->assertInstanceOf(AbstractConnection::class, $baseConnection->getConnection());
        } catch (AMQPConnectionException $e) {
            $this->fail($e->getMessage());
        }
    }
}
