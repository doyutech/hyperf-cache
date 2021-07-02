<?php

namespace Douyu\HyperfCache\Traits;

use Hyperf\Utils\Coroutine;
use Swoole\Coroutine as SwCoroutine;

trait BaseTrait
{
    /**
     * @param mixed ...$args
     * @return $this
     */
    public static function getInstance(...$args): ?self
    {
        $class = static::class;
        if(Coroutine::inCoroutine())
        {
            //协程情况下
            $coId = Coroutine::id();
            $key = $coId.'--'.md5( $class . json_encode($args));

            if(!isset(SwCoroutine::getContext()[$key]))
            {
                SwCoroutine::getContext()[$key] = new $class(...$args);
            }

            return SwCoroutine::getContext()[$key] ?? null;
        }
        return new $class(...$args);
    }
}