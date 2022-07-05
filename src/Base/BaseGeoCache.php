<?php

namespace Douyu\HyperfCache\Base;

/**
 * Geo缓存抽象类
 * Class BaseGeoCache
 * @package App\Cache
 */
abstract class BaseGeoCache extends BaseCache
{

    public function __construct($pk = '', string $redisPool = 'default')
    {
        parent::__construct($pk);

        $this->setHandle('Geo');

        $this->setRedisPool($redisPool);
    }
    
    protected function fromDb(){}
    
    /**
     * 存储对象地理位置信息
     * @param string $member    对象
     * @param float $longitude  经度
     * @param float $latitude   纬度
     * @return int
     */
    public function geoAdd(string $member, float $longitude, float $latitude)
    {
        return self::getRedis($this->getRedisPool())->geoadd($this->cacheKey, $longitude, $latitude, $member);
    }
    
    /**
     * 获取当前对象的地理位置信息
     * @param string $member
     * @return array
     */
    public function geoPos(string $member)
    {
        return self::getRedis($this->getRedisPool())->geopos($this->cacheKey, $member);
    }
    
    /**
     * 计算两个位置之间的距离
     * @param string $member1
     * @param string $member2
     * @param null $unit 默认m 'm' => Meters（米）, km' => Kilometers（千米）, mi' => Miles（英里）, 'ft' => Feet（英尺）
     * @return float
     */
    public function geoDist(string $member1, string $member2, $unit = null)
    {
        return self::getRedis($this->getRedisPool())->geodist($this->cacheKey, $member1, $member2, $unit);
    }
    
    /**
     * 根据用户给定的经纬度坐标来获取指定范围内的地理位置集合
     * @param float $longitude
     * @param float $latitude
     * @param $radius //半径
     * @param string $unit 默认m 'm' => Meters（米）, km' => Kilometers（千米）, mi' => Miles（英里）, 'ft' => Feet（英尺）
     * @param array|null $options
     * ['WITHDIST'] 返回元素的同时，返回与指定经纬度的距离，距离单位与上述给定单位一致
     * ['WITHCOORD'] 同时返回元素的经纬度信息
     * ['WITHHASH'] 返回Geohash值
     * ['ASC']：距给定经纬度由近及远的顺序
     * ['DESC']：距给定经纬度由远及近的顺序
     * @return mixed
     */
    public function geoRadius(float $longitude, float $latitude, $radius, string $unit, array $options = null)
    {
        return self::getRedis($this->getRedisPool())->georadius($this->cacheKey, $longitude, $latitude, $radius, $unit, $options);
    }
    
    /**
     * 根据储存在位置集合里面的某个地点获取指定范围内的地理位置集合
     * @param string $member
     * @param $radius
     * @param string $unit
     * @param array|null $options
     * @return array
     */
    public function geoRadiusByMember(string $member, $radius, string $unit, array $options = null)
    {
        return self::getRedis($this->getRedisPool())->georadiusbymember($this->cacheKey, $member, $radius, $units, $options);
    }
    
    /**
     * 返回一个或多个位置对象的geohash值
     * @param ...$member
     * @return array
     */
    public function geoHash(...$member)
    {
        return self::getRedis($this->getRedisPool())->geohash($this->cacheKey, ...$member);
    }
    
    /**
     * 移除对象的地理位置信息
     * @param $member
     * @return int
     */
    public function geoRemove(array $members)
    {
        return self::getRedis($this->getRedisPool())->zRem($this->cacheKey, ...$members);
    }
}