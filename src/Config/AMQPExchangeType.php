<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 14
 * Time: 11:41
 */

namespace MaxSky\AMQP\Config;

final class AMQPExchangeType {
    const DIRECT = 'direct';
    const FANOUT = 'fanout';
    const TOPIC = 'topic';
    const HEADERS = 'headers';

    const DELAYED = 'x-delayed-message';
}
