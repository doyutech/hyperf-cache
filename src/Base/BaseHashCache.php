<?php

namespace Douyu\HyperfCache\Base;

/**
 * Hash缓存抽象类
 * Class BaseHashCache
 * @package App\Cache
 */
abstract class BaseHashCache extends BaseCache
{

    public function __construct($pk = '', string $redisPool = 'default')
    {
        parent::__construct($pk);

        $this->setHandle('Hash');
        
        $this->setRedisPool($redisPool);

    }

}