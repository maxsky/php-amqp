<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 14
 * Time: 11:33
 */

namespace MaxSky\AMQP\Handler;

use MaxSky\AMQP\Exception\AMQPMessageHandlerException;

interface MessageHandlerInterface {

    /**
     * @param mixed $data
     * @param mixed $result
     *
     * @return void
     * @throws AMQPMessageHandlerException
     */
    public function handle($data, &$result = null): void;

    /**
     * @param array $data
     *
     * @return void
     */
    public function failed(array $data): void;
}
