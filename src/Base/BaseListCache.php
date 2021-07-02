<?php

namespace Douyu\HyperfCache\Base;

/**
 * List缓存抽象类
 * Class BaseListCache
 * @package App\Cache
 */
abstract class BaseListCache extends BaseCache
{

    public function __construct($pk = '', string $redisPool = 'default')
    {
        parent::__construct($pk);

        $this->setHandle('List');

        $this->setRedisPool($redisPool);
    }

    protected function fromDb(){}
    
    /**
     * lPush
     * @param $member
     * @return false|int
     */
    public function push($member)
    {
        return self::getRedis($this->redisPool)->lPush($this->cacheKey, $member);
    }
    
    /**
     * rPop
     * @return bool|mixed
     */
    public function pop()
    {
        return self::getRedis($this->redisPool)->rPop($this->cacheKey);
    }
}