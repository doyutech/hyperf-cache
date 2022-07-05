<?php

namespace Douyu\HyperfCache\Base;

/**
 * sortedSet缓存抽象类
 * Class BaseSortSetCache
 * @package App\Cache
 */
abstract class BaseSortSetCache extends BaseCache
{
    
    public function __construct($pk = '', string $redisPool = 'default')
    {
        parent::__construct($pk);
        
        $this->setHandle('SortedSet');    //有序集合
        
        $this->setRedisPool($redisPool);
    }
    
    protected function fromDb() {}
    
    /**
     * @param array ...$args
     * @return string
     */
    protected function getListCacheKey(...$args): string
    {
        return $this->cacheKey;
    }
    
    //region ---对象下的列表缓存---
    
    /**
     *  加入列表缓存
     * @param string|int $itemPk 数据标识，一般是主键
     * @param int $sort
     * @param array ...$args
     * @return bool|int
     */
    public function join($itemPk, $sort = 0, ...$args)
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return false;
        }
        if (!$itemPk) {
            return false;
        }
        return self::getRedis($this->getRedisPool())->zAdd($this->getListCacheKey(...$args), $sort, $itemPk);
    }
    
    /**
     * 是否存在
     * @param string|int $itemPk 数据标识，一般是主键
     * @param array ...$args
     * @return bool|int
     */
    public function has($itemPk, ...$args)
    {
        if ((empty($this->pk)  && !$this->allowPkNull) || empty($itemPk)) {
            return 0;
        }
        return self::hasInSortedList($this->getRedisPool(), $this->getListCacheKey(...$args), $itemPk);
    }
    
    /**
     * 从列表缓存中移除某项
     * @param string|int $itemPk 数据标识，一般是主键
     * @param array ...$args
     * @return bool|int
     */
    public function remove($itemPk, ...$args)
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return false;
        }
        if (!$itemPk) {
            return false;
        }
        
        return self::getRedis($this->getRedisPool())->zRem($this->getListCacheKey(...$args), $itemPk);
    }
    
    /**
     * 删除多个zset的member
     * @param array $members
     * @return false|int
     */
    public function removeMulti(array $members)
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return false;
        }
        return self::getRedis($this->getRedisPool())->zRem($this->getListCacheKey(), ...$members);
    }
    
    /**
     * 删除zset中member<$maxMember的元素,不包含$maxMember
     * @param int $maxMember
     * @return false
     */
    public function removeByLex(int $maxMember): bool
    {
        if ( (empty($this->pk)  && !$this->allowPkNull) || $maxMember <= 0) {
            return false;
        }
        return self::getRedis($this->getRedisPool())->zRemRangeByLex($this->getListCacheKey(), '-', '(' . $maxMember);
    }
    
    /**
     * 获取列表缓存的元素数量
     * @param array ...$args
     * @return int
     */
    public function getLength(...$args): int
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return 0;
        }
        return self::getSortedListLen($this->getRedisPool(), $this->getListCacheKey(...$args));
    }
    
    /**
     * 获取列表缓存的元素数量（在[$startScore,$endScore]之间）
     * @param array ...$args
     * @param int $startScore
     * @param int $endScore
     * @return int
     */
    public function getLengthExt($startScore = 0, $endScore = 0, ...$args): int
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return 0;
        }
        return self::getSortedListLenExt($this->getRedisPool(), $this->getListCacheKey(...$args), $startScore, $endScore);
    }
    
    
    public function getLengthByLex(int $startMember = 0, int $endMember = 0, ...$args): int
    {
        if (empty($this->pk) && !$this->allowPkNull) return 0;
        return self::getSortedListLenByLex($this->getRedisPool(), $this->getListCacheKey(...$args), $startMember, $endMember);
    }
    
    /**
     * 获取列表
     * @param int $startRow
     * @param int $row
     * @param int $sortType
     * @param false $withScore
     * @param null $startScore
     * @param null $endScore
     * @param ...$args
     * @return array
     */
    public function getList($startRow = 0, $row = 0, $sortType = 1, $withScore = false, $startScore = null, $endScore = null, ...$args): array
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return [];
        }
        //        self::isReset($this->cacheKey);
        
        if($sortType == 2){
            $score = $startScore;
            $startScore = $endScore;
            $endScore = $score;
        }
        return self::getSortedListKey(
            $this->getRedisPool(),
            $this->getListCacheKey(...$args),
            $startRow,
            $row,
            $startScore,
            $endScore,
            $withScore,
            $sortType
        );
    }
    
    /**
     * 获取列表
     * @param int $startScore
     * @param int $endScore
     * @param int $row
     * @param int $sortType
     * @param bool $withScore
     * @param array ...$args
     * @return array
     */
    public function getListExt($startScore = 0, $endScore = 0, $row = 10, $sortType = 1, $withScore = false, ...$args): array
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return [];
        }
        //        self::isReset($this->cacheKey);
        
        return self::getSortedListKey(
            $this->getRedisPool(),
            $this->getListCacheKey(...$args),
            0,
            $row,
            $startScore,
            $endScore,
            $withScore,
            $sortType
        );
    }
    
    /**
     * 依据zset的member范围获取列表
     * @param int $sortType
     * @param null $startMember
     * @param null $endMember
     * @param null $offset
     * @param null $limit
     * @param mixed ...$args
     * @return array
     */
    public function getListByLex($sortType = 1, $startMember = null, $endMember = null, $offset = null, $limit = null, ...$args): array
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return [];
        }
        
        if ($sortType == 2) {
            $member = $startMember;
            $startMember = $endMember;
            $endMember = $member;
        }
        return self::getSortedListKeyByLex(
            $this->getRedisPool(),
            $this->getListCacheKey(...$args),
            $startMember,
            $endMember,
            $offset,
            $limit,
            $sortType
        );
    }
    
    /**
     * 删除列表缓存
     * @param array ...$args
     * @return int
     */
    public function delList(...$args)
    {
        if (empty($this->pk) && !$this->allowPkNull) {
            return false;
        }
        return self::getRedis($this->getRedisPool())->del($this->getListCacheKey(...$args));
    }
    
    /**
     * 设置过期时间
     * @param $ttl
     * @param ...$args
     * @return bool
     */
    public function expire($ttl = 0, ...$args): bool
    {
        $ttl = $ttl ?? $this->ttl;
        return self::getRedis($this->getRedisPool())->expire($this->getListCacheKey(...$args), $ttl);
    }
    //endregion
}