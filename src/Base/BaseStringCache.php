<?php

namespace Douyu\HyperfCache\Base;

/**
 * string缓存抽象类
 * Class BaseStringCache
 * @package App\Cache
 */
abstract class BaseStringCache extends BaseCache
{

    public function __construct($pk = '', string $redisPool = 'default')
    {
        parent::__construct($pk);

        $this->setHandle('String');

        $this->setRedisPool($redisPool);
    }

}