<?php


namespace Douyu\HyperfCache\Base;

/**
 * UnsortedSet缓存抽象类
 * Class BaseUnSortSetCache
 * @package App\Cache\Base
 */
abstract class BaseUnSortSetCache extends BaseCache
{

    public function __construct($pk = '', string $redisPool = 'default')
    {
        parent::__construct($pk);

        $this->setHandle('Set');    //无序集合

        $this->setRedisPool($redisPool);
    }

    protected function fromDb(){}

    //region ---对象下的列表缓存---
    /**
     *  加入列表缓存
     * @param string|int $itemPk    数据标识，一般是主键
     * @return bool|int
     */
    public function join($itemPk)
    {
        if(empty($this->pk) && !$this->allowPkNull) return false;
        if(!$itemPk) return false;
        return self::getRedis($this->getRedisPool())->sAdd($this->cacheKey, $itemPk);
    }

    /**
     * 批量加入缓存列表
     * @param array $itemPKs
     * @return bool|int
     */
    public function joinAll(array $itemPKs)
    {
        if (empty($itemPKs)) return false;
        if (empty($this->pk) && !$this->allowPkNull) return false;
        if (empty($this->cacheKey)) return false;
        return self::getRedis($this->getRedisPool())->sAddArray($this->cacheKey, $itemPKs);
    }

    /**
     * 是否存在
     * @param string|int $itemPk    数据标识，一般是主键
     * @return bool|int
     */
    public function has($itemPk)
    {
        if( (empty($this->pk) && !$this->allowPkNull) || empty($itemPk)) return 0;
        return self::hasInUnSortedList($this->getRedisPool(), $this->cacheKey, $itemPk);
    }

    /**
     * 从列表缓存中移除某项
     * @param string|int $itemPk    数据标识，一般是主键
     * @return bool|int
     */
    public function remove($itemPk)
    {
        if(empty($this->pk) && !$this->allowPkNull) return false;
        if(!$itemPk) return false;
        return self::getRedis($this->getRedisPool())->sRem($this->cacheKey , $itemPk);
    }

    /**
     * 获取列表缓存的元素数量
     * @return int
     */
    public function getLength(): int
    {
        if(empty($this->pk) && !$this->allowPkNull) return 0;
        return self::getUnSortedListLength($this->getRedisPool(), $this->cacheKey);
    }

    /**
     * 获取列表
     * @return array
     */
    public function getList(): array
    {
        if(empty($this->pk) && !$this->allowPkNull) return [];
//        self::isReset($this->cacheKey);

        return self::getRedis($this->getRedisPool())->sMembers($this->cacheKey);
    }

    /**
     * 删除列表缓存
     * @return int
     */
    public function delList()
    {
        if(empty($this->pk) && !$this->allowPkNull) return false;
        return self::getRedis($this->getRedisPool())->del($this->cacheKey);
    }

    /**
     * 获取指定长度的列表
     * @param int $count
     * @return array|bool|mixed|string
     */
    public function getListByLex($count = 1)
    {
        if(empty($this->pk) && !$this->allowPkNull) return [];
//        self::isReset($this->cacheKey);

        return self::getRedis($this->getRedisPool())->sRandMember($this->cacheKey, $count);
    }
    //endregion
}