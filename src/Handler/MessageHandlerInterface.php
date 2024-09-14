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
     * @param mixed     $data
     * @param bool|null $result
     *
     * @return void
     * @throws AMQPMessageHandlerException
     */
    public function handle($data, ?bool &$result = null): void;

    /**
     * @param array $data
     *
     * @return void
     */
    public function failed(array $data): void;
}
