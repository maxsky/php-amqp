<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 18
 * Time: 11:03
 */

namespace Tests\Message;

use MaxSky\AMQP\Handler\MessageHandlerInterface;

class ExampleMessageHandler implements MessageHandlerInterface {

    public function handle($data, &$result = null): void {
        print_r($data);

        $result = true;
    }

    public function failed(array $data): void {
        echo 'Message handle failed.' . PHP_EOL;
    }
}
