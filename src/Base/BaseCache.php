<?php

namespace Douyu\HyperfCache\Base;

use Closure;
use Douyu\HyperfCache\Traits\BaseTrait;
use Douyu\HyperfCache\Traits\RedisTrait;
use Douyu\HyperfCache\Utils\LockUtil;

/**
 * 基本缓存详情数据类
 * Class BaseCache
 * @package App\Cache
 */
abstract class BaseCache
{
    use BaseTrait;
    use RedisTrait;
    
    /**
     * Redis连接池
     * @var string
     */
    protected string $redisPool = 'default';
    /**
     * @var string|int $pk
     */
    protected $pk;
    /**
     * @var string 缓存键名
     */
    protected string $cacheKey;
    /**
     * @var int 缓存有效时间，单位：秒
     */
    protected int $ttl = 365 * 24 * 60 * 60;
    /**
     * @var mixed 详情数据
     */
    protected $detail = null;
    /**
     * @var BaseSortSetCache|BaseUnSortSetCache 相关列表缓存类
     */
    protected $related = [];
    /**
     * 数据类型handle
     * @var ?string
     */
    protected ?string $handle = null;
    
    /**
     * @var array 整型字段
     */
    protected array $intFieldArr = [];
    
    /**
     * 是否允许pk为空
     * @var bool
     */
    protected bool $allowPkNull = false;
    
    /**
     * 是否序列化存储，默认false:json，true:serialize
     * @var bool
     */
    protected bool $serialize = false;
    
    public function __construct($pk = '')
    {
        $this->pk = $pk;
        
        $this->setCacheKey();
    }
    
    /**
     * 设置缓存key
     * @return mixed
     */
    abstract protected function setCacheKey();
    
    /**
     * 从数据库中查询数据
     * @return mixed
     */
    abstract protected function fromDb();
    
    /**
     * 更新或重构缓存额外处理
     * @param mixed $data
     */
    protected function dealUpdateCacheExt(&$data){}
    
    /**
     * 额外处理，比如ES处理
     */
    protected function saveCacheExt(){}
    
    /**
     * 额外处理，比如ES处理
     */
    protected function delCacheExt(){}
    
    /**
     * 保存前数据处理
     * @param $data
     * @return void
     */
    protected function dealSaveCacheExt(&$data){}
    
    /**
     * 清除所有相关缓存
     */
    public static function clearAll(){}
    
    /**
     * 重构所有相关缓存
     */
    public static function reset(){}
    
    /**
     * 获取redis连接池代理
     * @param string
     */
    public function setRedisPool(string $redisPool)
    {
        $this->redisPool = $redisPool;
    }
    
    /**
     * 获取redis连接池代理
     * @return string
     */
    public function getRedisPool(): string
    {
        return $this->redisPool;
    }
    
    /**
     * @param string $handle
     * @return void
     */
    public function setHandle(string $handle)
    {
        $this->handle = $handle;
    }
    
    /**
     *
     * @return string
     */
    public function getHandle(): string
    {
        return $this->handle;
    }
    
    /**
     * @return int|string
     */
    public function getPk()
    {
        return $this->pk;
    }
    
    /**
     * @param int|string $pk
     * @return void
     */
    public function setPk($pk)
    {
        $this->pk = $pk;
    }
    
    /**
     * 判断PK是否为空
     * @return string
     */
    protected function hasPk()
    {
        if ($this->allowPkNull) {
            return true;
        }
        return $this->pk;
    }
    
    /**
     * 是否为空数据
     * @return bool
     */
    protected function isDetailNull(): bool
    {
        if ($this->detail === array(null) || $this->detail === '[]'
            || $this->detail === array('') || $this->detail === 'null') {
            $this->detail = null;
            return true;
        }
        
        return false;
    }
    
    /**
     * 获取数据详情。
     * 优先从缓存获取，不存在则从数据库中获取再存入缓存中
     * @param array $columns
     * @return array|mixed|null
     * @throws
     */
    public function getDetail(array $columns = [])
    {
        if (!$this->hasPk()) {
            return null;
        }
        
        $this->getCache($columns);
        
        if (empty($this->detail)) {
            $this->getDetailWhereNoCache();
            if ($columns) {
                $this->detail = filterArrKeys($this->detail, $columns);
            }
        }
        /*else {
            //$this->ttl(); // 不再自动续期
        }*/
        
        $this->dealDetailExt($this->detail);
        empty($this->detail) && ($this->detail = []);
        return $this->detail;
    }
    
    /**
     * 判别缓存是否存在
     * @return bool
     */
    public function exists(): bool
    {
        if (! $this->hasPk()) return false;
        $aRet = $this->getRedis($this->redisPool)->exists($this->cacheKey);
        if (is_bool($aRet)) return $aRet;
        return $aRet > 0;
    }
    
    /**
     * 当缓存无数据时，从数据库获取数据
     * @return mixed|null
     * @throws
     */
    public function getDetailWhereNoCache()
    {
        if ($this->isNull($this->cacheKey)) {
            return null;
        }
        //加锁（互斥锁）,解决缓存击穿问题，预防缓存失效时高并发读取数据库
        LockUtil::lock(
            $this->redisPool,
            $this->cacheKey,
            5,
            10,
            function () {
                $this->getCache();
                if (!$this->detail && !$this->isNull($this->cacheKey)) {
                    $this->buildCache();
                }
            }
        );
        
        return $this->detail;
    }
    
    /**
     * 获取指定字段
     * @param array $columns
     * @return array|mixed|null
     * @throws
     */
    public function getColumns(array $columns = [])
    {
        if (!$this->hasPk())
        {
            return null;
        }
        $detail = $this->getDetail($columns);
        return $detail ?: null;
    }
    
    /**
     * 从缓存中获取数据
     * @param array $columns
     * @return void
     */
    protected function getCache(array $columns = [])
    {
        if ($columns)
        {
            if ($this->getHandle() == 'String') {
                $detail = self::getRedis($this->redisPool)->get($this->cacheKey);
                if ($this->serialize) {
                    $detail = unserialize($detail);
                } else {
                    $detail = json_decode($detail, true);
                }
                $this->detail = filterArrKeys($detail, $columns);
            } else {
                $this->detail = self::getRedis($this->redisPool)->hMGet($this->cacheKey, $columns);
                $this->detail = array_filter($this->detail, function($val, $key) {
                    if (false === $val) {
                        return false;
                    }
                    return true;
                }, ARRAY_FILTER_USE_BOTH);
            }
        }
        else
        {
            if ($this->getHandle() == 'String') {
                $this->detail = self::getRedis($this->redisPool)->get($this->cacheKey);
                if ($this->serialize) {
                    $this->detail = unserialize($this->detail);
                } else {
                    $this->detail = json_decode($this->detail, true);
                }
            } else {
                $this->detail = self::getRedis($this->redisPool)->hGetAll($this->cacheKey);
            }
        }
    }
    
    /**
     * 获取某字段的缓存值
     * @param $field
     * @return mixed|null
     * @throws
     */
    public function field($field)
    {
        if (!$this->hasPk()) {
            return null;
        }
        
        $this->getDetail();
        
        return $this->detail[$field] ?? null;
    }
    
    /**
     * @param $name
     * @return mixed|null
     * @throws
     */
    public function __get($name)
    {
        return $this->field($name);
    }
    
    /**
     * @param $name
     * @param $value
     * @return bool
     * @throws
     */
    public function __set($name, $value)
    {
        return $this->updateFieldCache($name, $value);
    }
    
    public function __isset($name)
    {
    
    }
    
    /**
     * 数据额外处理
     * @param $detail
     */
    public function dealDetailExt(&$detail)
    {
        //整型字段处理
        $this->intFieldArr = array_merge($this->intFieldArr, [
            'status', 'sort'        //'created_at', 'updated_at',
        ]);
        
        foreach ($this->intFieldArr as $field)
        {
            isset($detail[$field]) && $detail[$field] = (int)$detail[$field];
        }
        if (isset($detail['deleted_at'])) {
            unset($detail['deleted_at']);
        }
    }
    
    /**
     * 清除缓存及内存中的数据
     * @return int
     */
    public function clearCache(): int
    {
        if (!$this->hasPk()) {
            return 0;
        }
        $this->detail = null;
        return self::getRedis($this->redisPool)->del($this->cacheKey);
    }
    
    /**
     * 创建/重构缓存数据
     * @return null
     * @throws
     */
    public function buildCache()
    {
        if (!$this->hasPk()) {
            return null;
        }
        
        $this->clearCache();
        
        $object = $this->fromDb();
        if (!$object) {
            //不存在，则直接置为null
            $this->setNull($this->cacheKey, 5);
            return null;
        } else {
            if (is_object($object)) {
                if (get_class($object) == 'stdClass') {
                    $this->detail = get_object_vars($object);
                } else {
                    $this->detail = json_decode($object, true);
                }
            } else {
                $this->detail = $object;
            }
        }
        $this->dealUpdateCacheExt($this->detail);
        
        $this->saveCache();
    }
    
    
    /**
     * 保存数据到缓存中
     * @throws
     */
    protected function saveCache()
    {
        if (!$this->detail) {
            return;
        }
        $this->dealSaveCacheExt($this->detail);
        
        $this->removeNull($this->cacheKey);
        
        if ($this->getHandle() == 'String') {
            if ($this->serialize) {
                $data = serialize($this->detail);
            } else {
                $data = json_encode($this->detail);
            }
            self::getRedis($this->redisPool)->set($this->cacheKey, $data);
        } else {
            self::getRedis($this->redisPool)->hMSet($this->cacheKey, $this->detail);
        }
        $this->ttl();
        
        $this->saveCacheExt();
    }
    
    /**
     * 更新缓存过期时间
     * @throws
     */
    protected function ttl(): void
    {
        if ($this->ttl <= 0) {
            return;
        }
        
        if ($this->isDetailNull()) {
            $ttl = 60;
        } else {
            //过期时间加上随机数，解决缓存雪崩问题
            $ttl = $this->ttl + random_int(0, 10);
        }
        
        self::getRedis($this->redisPool)->expire($this->cacheKey, $ttl);
    }
    
    /**
     * 是否存在缓存
     * @return bool|int
     */
    private function hasCache()
    {
        return self::getRedis($this->redisPool)->exists($this->cacheKey);
    }
    
    /**
     * 字段增长（整型增长）
     * @param string $field
     * @param int $incr
     * @return false|int|null
     */
    public function incr(string $field, $incr = 1)
    {
        if (!$this->hasPk()) {
            return null;
        }
        
        if ($this->hasCache()) {
            $this->getCache([$field]);
            
            if (!isset($this->detail[$field])) {
                return false;
            }
            $this->detail[$field] = self::getRedis($this->redisPool)->hIncrBy($this->cacheKey, $field, $incr);
            
            $data = [$field => $this->detail[$field]];
            $this->dealUpdateCacheExt($data);
            
            $this->ttl();
            
            $this->saveCacheExt();
        } else {
            $this->buildCache();
        }
        
        return $this->detail[$field];
    }
    
    /**
     * 字段增长（浮点数增长）
     * @param string $field
     * @param float $incr
     * @return false|float|null
     */
    public function incrByFloat(string $field, $incr = 1.0)
    {
        if (!$this->hasPk()) {
            return null;
        }
        
        if ($this->hasCache()) {
            $this->getCache([$field]);
            
            if (!isset($this->detail[$field])) {
                return false;
            }
            $this->detail[$field] = self::getRedis($this->redisPool)->hIncrByFloat($this->cacheKey, $field, $incr);
            
            $data = [$field => $this->detail[$field]];
            $this->dealUpdateCacheExt($data);
            
            $this->ttl();
            
            $this->saveCacheExt();
        } else {
            $this->buildCache();
        }
        
        return $this->detail[$field];
    }
    
    /**
     * 更新某个字段的缓存
     * @param string $field
     * @param $value
     * @return bool
     * @throws
     */
    public function updateFieldCache(string $field, $value): bool
    {
        if (!$this->hasPk()) {
            return false;
        }
        
        $this->getDetail();
        
        if (!isset($this->detail[$field])) {
            return false;
        }
        
        if ($this->getHandle() == 'String') {
            $data = $this->detail;
            $data[$field] = $value;
            if ($this->serialize) {
                $data = serialize($data);
            } else {
                $data = json_encode($data);
            }
            self::getRedis($this->redisPool)->set($this->cacheKey, $data);
        } else {
            self::getRedis($this->redisPool)->hSet($this->cacheKey, $field, $value);
        }
        
        $data = [$field => $this->detail[$field]];
        $this->dealUpdateCacheExt($data);
        
        $this->ttl();
        
        $this->detail[$field] = $value;
        
        $this->saveCacheExt();
        
        return true;
    }
    
    /**
     * 更新多个字段的缓存
     * @param array $data
     * @return bool
     * @throws
     */
    public function updateMulFieldCache(array $data): bool
    {
        if (!$this->hasPk()) {
            return false;
        }
        
        $this->getDetail();
        if (empty($this->detail)) {
            return false;
        }
        
        foreach ($data as $field => $value) {
            if (!isset($this->detail[$field])) {
                unset($data[$field]);
            }
        }
        
        if (empty($data)) {
            return false;
        }
        
        $this->detail = array_merge($this->detail, $data);
        
        $this->dealUpdateCacheExt($data);
        
        $this->saveCache();
        
        return true;
    }
    
    /**
     * 直接设置缓存详情并保存
     * @param $detail
     * @throws
     */
    public function setDetail($detail)
    {
        if (!$this->hasPk()) {
            return;
        }
        
        if (is_object($detail)) {
            if (get_class($detail) == 'stdClass') {
                $detail = get_object_vars($detail);
            } else {
                $detail = json_decode($detail, true);
            }
        }
        $this->detail = $detail;
        $this->dealUpdateCacheExt($this->detail);
        $this->saveCache();
    }
    
    /**
     * 清除详情缓存
     */
    public function delCache()
    {
        if (!$this->hasPk()) {
            return;
        }
        $this->detail = null;
        self::getRedis($this->redisPool)->del($this->cacheKey);
        
        $this->delCacheExt();
    }
    
    /**
     * 判断缓存是否存在
     * @return bool
     * @throws
     */
    public function isEmpty(): bool
    {
        $detail = $this->getDetail();
        return !$detail;
    }
    
    //region ---关联列表缓存类处理---
    
    /**
     * @param $class
     * @return BaseSortSetCache|BaseUnSortSetCache
     */
    public function getRelated($class)
    {
        if (!isset($this->related[$class])) {
            $this->related[$class] = new $class($this->getRedisPool(), $this->pk);
        }
        
        return $this->related[$class];
    }
    //endregion
    
    /**
     * 是否为空缓存
     * @param $key
     * @return bool|int
     */
    protected function isNull($key)
    {
        return self::getRedis($this->redisPool)->exists($this->getNullKey($key));
    }
    
    /**
     * 设置空缓存，解决缓存穿透的问题
     * @param $key
     * @param int $ttl
     * @throws
     */
    protected function setNull($key, int $ttl = 60)
    {
        $ttl = $ttl + random_int(0, 10);
        self::getRedis($this->redisPool)->setex($this->getNullKey($key), $ttl, 1);
    }
    
    /**
     * 获取空缓存键名
     * @param $key
     * @return string
     */
    private function getNullKey($key): string
    {
        return $key . '.null';
    }
    
    /**
     * 移除空缓存
     * @param $key
     */
    public function removeNull($key)
    {
        self::getRedis($this->redisPool)->del($this->getNullKey($key));
    }
    
    /**
     * 是否重置缓存
     * @param string $listKey
     * @param Closure|null $closure
     * @return bool
     */
    protected static function isReset(string $listKey, Closure $closure = null): bool
    {
        return false;
        /* return LockUtil::lock(
            $listKey,
            60,
            5,
            function () use ($listKey, $closure) {
                if (!self::getRedis()->exists($listKey) || self::isNull($listKey)) {
                    //若Redis不存在列表key，则重构缓存
                    if ($closure) {
                        $ret = $closure();
                        !$ret && self::setNull($listKey);
                    }
                    return true;
                }
                return false;
            }
        );*/
    }
    
    /**
     * 计算排序值，sort值升序，时间倒序
     * @param $item
     * @return float|int
     */
    public static function calSort($item)
    {
        $pkField = static::getPkField();
        if (is_array($item)) {
            return ($item['sort'] + 1) * 10000000000 + $item[$pkField];
        }
        
        return ($item->sort + 1) * 10000000000 + $item->$pkField;
    }
    
    /**
     * 获取主键字段名
     * @return string
     */
    protected static function getPkField(): string
    {
        return 'id';
    }
    
    /**
     * 批量获取详情缓存
     * @param string $poolName
     * @param array $pkArr
     * @param array $fieldArr
     * @param string $keyName 键字段名
     * @return array|void
     */
    public static function getDetailMulti(string $poolName, array $pkArr, $fieldArr = [], $keyName = ''): array
    {
        if (empty($pkArr))
        {
            return [];
        }
        
        $key = 0;
        $cacheObjArr = [];
        $cachePkField = static::getPkField(); // 缓存主键
        // 如果指定字段列表,需要加上表主键
        if ($fieldArr && !in_array($cachePkField, $fieldArr))
        {
            array_unshift($fieldArr, $cachePkField);
        }
        // 取缓存
        $redisPipe = self::getRedis($poolName)->multi(\Redis::PIPELINE);
        foreach ($pkArr as $pk)
        {
            $cacheObj = new static($pk, $poolName);
            // 取部分字段
            if ($cacheObj->getHandle() == 'String') {
                $redisPipe->get($cacheObj->cacheKey);
            } else {
                if ($fieldArr) {
                    $redisPipe->hMGet($cacheObj->cacheKey, $fieldArr);
                } // 全部字段
                else {
                    $redisPipe->hGetAll($cacheObj->cacheKey);
                }
            }
            $cacheObjArr[$key++] = $cacheObj;
        }
        $list = $redisPipe->exec();
        
        // 处理结果
        $ret = [];
        // 指定键字段名，且在查询字段范围或全部字段
        $pointedKeyName = $keyName && (in_array($keyName, $fieldArr) || empty($fieldArr));
        
        foreach ($list as $key => $item)
        {
            if ($item && $cacheObjArr[$key]->getHandle() == 'String') {
                if ($cacheObjArr[$key]->serialize) {
                    $item = unserialize($item);
                } else {
                    $item = json_decode($item, true);
                }
            }
            // 如果当前key没有命中缓存,取全部字段时,item=[],取部分字段时,其字段全等于false(此时使用缓存主键判别)
            if (!$item || false === $item[$cachePkField])
            {
                $item = $cacheObjArr[$key]->getDetailWhereNoCache();
            }
            // 如果还是没有找到,直接忽略
            if ($item)
            {
                $cacheObjArr[$key]->dealDetailExt($item);
                
                if ($fieldArr)
                {
                    $item = filterArrKeys($item, $fieldArr);
                }
                if ($pointedKeyName && isset($item[$keyName]))
                {
                    $keyValue = $item[$keyName];
                    $ret[$keyValue] = $item;
                }
                else
                {
                    $ret[] = $item;
                    $pointedKeyName = false;
                }
            }
        }
        unset($cacheObjArr);
        return $ret;
    }
}