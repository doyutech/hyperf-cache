<?php

namespace Douyu\HyperfCache\Traits;

use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use Hyperf\Utils\ApplicationContext;

/**
 * Redis工具Trait
 * Trait RedisTrait
 * @package App\Traits
 */
trait RedisTrait
{
    /**
     * @var RedisProxy $instance
     */
    private static $redis = null;
    
    /**
     * 初始化Redis
     */
    private static function initRedis(string $poolName = 'default')
    {
        self::$redis = ApplicationContext::getContainer()->get(RedisFactory::class)->get($poolName);
    }
    
    /**
     * 获取Redis连接对象
     * @param string $poolName
     * @return RedisProxy
     */
    public static function getRedis(string $poolName = 'default'): RedisProxy
    {
        
        try {
            self::initRedis($poolName);
        } catch (\Exception $exception) {
            self::initRedis($poolName);
        }
        
        return self::$redis;
    }
    
    /**
     * setEx
     * @param string $poolName
     * @param $key
     * @param $value
     * @param $ttl
     * @return bool
     */
    public static function setEx(string $poolName, $key, $value, $ttl): bool
    {
        return self::getRedis($poolName)->set($key, $value, ['nx', 'ex' => $ttl]);
    }
    
    /**
     * incrEx
     * @param string $poolName
     * @param $key
     * @param $ttl
     * @return int
     */
    public static function incrEx(string $poolName, $key, $ttl): int
    {
        $count = self::getRedis($poolName)->incr($key);
        // 第一次增加
        if (1 == $count)
        {
            try
            {
                self::getRedis($poolName)->expire($key, $ttl);
            }
            catch (\Exception $ex)
            {
                self::getRedis($poolName)->expire($key, $ttl);
            }
            finally
            {
                // 发现永久有效
                if (-1 == self::getRedis($poolName)->ttl($key))
                {
                    try
                    {
                        $ret = self::getRedis($poolName)->expire($key, $ttl);
                        if (false === $ret)
                        {
                            throw new \Exception("服务异常，请稍后重试", 1001);
                        }
                    }
                    catch (\Exception $ex)
                    {
                        //throw new \Exception("服务异常，请稍后重试", 1001);
                    }
                }
            }
        }
        return $count;
    }
    
    /**
     * 获取有序列表的长度
     * @param string $poolName
     * @param string $key
     * @return int
     */
    public static function getSortedListLen(string $poolName, string $key): int
    {
        return self::getRedis($poolName)->zCard($key);
    }
    
    /**
     * 获取有序列表的长度（在[$startScore,$endScore]之间）
     * @param string $poolName
     * @param string $key
     * @param string $startScore score起始值，默认为0
     * @param string $endScore score截止值，默认为0
     * @return int
     */
    public static function getSortedListLenExt(string $poolName, string $key, string $startScore, string $endScore): int
    {
        return self::getRedis($poolName)->zCount($key, $startScore, $endScore);
    }
    
    /**
     * 获取有序列表的成员数量（在【$startMember，$endMember】之间）
     * @param string $poolName
     * @param string $key
     * @param int $startMember
     * @param int $endMember
     * @return int
     */
    public static function getSortedListLenByLex(string $poolName, string $key, int $startMember = 0, int $endMember = 0): int
    {
        return self::getRedis($poolName)->ZLexCount($key, $startMember ? ('[' . $startMember) : '-', $endMember ? ('[' . $endMember) : '+');
    }
    
    /**
     * 获取有序列表
     * @param string $poolName
     * @param string $key 列表键名
     * @param int $startRow 起始行
     * @param int $row 获取记录数，默认为10条
     * @param int $startScore score起始值，默认为0
     * @param int $endScore score截止值，默认为0
     * @param bool $withScores 是否返回score，true则返回，否则不返回
     * @param int $sortType 排序类型，1=顺序，2=倒序
     * @param int $skipNum 跳过数量，默认为1
     * @return array
     */
    public static function getSortedListKey(string $poolName, string $key, $startRow = 0, $row = 10, $startScore = 0, $endScore = 0,
                                                   $withScores = false, $sortType = 2, $skipNum = 1)
    {
        if ($startScore || $endScore) {
            $method = $sortType == 1 ? 'zRangeByScore' : 'zRevRangeByScore';
            
            $keyList = self::getRedis($poolName)->$method($key, $startScore, $endScore, [
                'limit' => [$startRow, $row],
                'withscores' => $withScores
            ]);
        } else {
            $endRow = $startRow + $row - 1;
            $endRow < -1 && $endRow = -1;
            
            $method = $sortType == 1 ? 'zRange' : 'zRevRange';
            
            $keyList = self::getRedis($poolName)->$method($key, $startRow, $endRow, $withScores);
        }
        
        return $keyList ?: [];
    }
    
    /**
     *
     * @param string $poolName
     * @param string $key
     * @param int $startMember
     * @param int $endMember
     * @param null $offset
     * @param null $limit
     * @param int $sortType
     * @return array
     */
    public static function getSortedListKeyByLex(
        string $poolName,
        string $key,
        int $startMember = 0,
        int $endMember = 0,
               $offset = null,
               $limit = null,
               $sortType = 2
    ): array
    {
        $method = $sortType == 1 ? 'zRangeByLex' : 'zRevRangeByLex';
        $keyList = self::getRedis($poolName)->$method($key, $startMember ? ('['.$startMember) : '-', $endMember ? ('['.$endMember) : '+', $offset, $limit);
        return $keyList ? $keyList : [];
    }
    
    /**
     * 判断有序列表中是否存在某成员
     * @param string $poolName
     * @param string $key
     * @param $value
     * @return bool
     */
    public static function hasInSortedList(string $poolName, string $key, $value): bool
    {
        return self::getRedis($poolName)->zScore($key, $value) === false ? false : true;
    }
    
    /**
     * 获取无序列表的长度
     * @param string $poolName
     * @param string $key
     * @return int
     */
    public static function getUnSortedListLength(string $poolName, string $key): int
    {
        return self::getRedis($poolName)->sCard($key);
    }
    
    /**
     * 获取无序列表
     * @param string $poolName
     * @param string $key
     * @return array
     */
    public static function getUnSortedList(string $poolName, string $key): array
    {
        return self::getRedis($poolName)->sMembers($key);
    }
    
    /**
     * 判断无序列表中是否存在某成员
     * @param string $poolName
     * @param string $key
     * @param $value
     * @return bool
     */
    public static function hasInUnSortedList(string $poolName, string $key, $value): bool
    {
        return self::getRedis($poolName)->sIsMember($key, $value);
    }
    
    /**
     * 根据前缀清除缓存
     * @param string $poolName
     * @param $keyPrefix
     */
    public static function clearKeys(string $poolName, $keyPrefix)
    {
        $redis = self::getRedis($poolName);
        
        $count = 1000;
        
        $iterator = null;
        $redis->setOption(\Redis::OPT_SCAN, \Redis::SCAN_RETRY);
        while ($arrKeys = $redis->scan($iterator, $keyPrefix . '*', $count)) {
            foreach ($arrKeys as $key) {
                $redis->del($key);
            }
        }
    }
    
    /**
     * 删除key
     * @param string $poolName
     * @param $keyField
     */
    public static function clearKey(string $poolName, $keyField)
    {
        self::getRedis($poolName)->del($keyField);
    }
    
}