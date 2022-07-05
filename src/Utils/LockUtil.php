<?php

namespace Douyu\HyperfCache\Utils;

use Douyu\HyperfCache\Traits\RedisTrait;
use Closure;
use Exception;

/**
 * 互斥锁类
 * Class LockUtil
 * @package App\Utils
 */
class LockUtil
{
    use RedisTrait;
    
    /**
     * 加锁（互斥锁）
     * @param string $poolName
     * @param string $key 待锁的键名
     * @param int $ttl 锁定时长，单位：秒
     * @param int $tryNum 尝试次数
     * @param Closure|null $closure 自定义的闭包函数
     * @return bool|mixed
     * @throws Exception
     */
    public static function lock(string $poolName, string $key, $ttl = 5, $tryNum = 0, Closure $closure = null)
    {
        $lockKey = self::getLockKey($key);
        do {
            if (self::setex($poolName, $lockKey, 1, $ttl)) {
                //获得锁
                if ($closure) {
                    $ret = $closure();
                    self::delLock($poolName, $key);
                    return $ret;
                }
                return true;
            } else if ($tryNum) {
                //未获得锁，协程阻塞
                --$tryNum;
                \Swoole\Coroutine\System::sleep(0.1);
            } else {
                //未获得锁，不阻塞
                break;
            }
        } while ($tryNum > 0);
        throw new Exception('System Busy Error！', 1006);
    }
    
    /**
     * 获取锁键名
     * @param string $key
     * @return string
     */
    private static function getLockKey(string $key): string
    {
        return $key . '.lock';
    }
    
    /**
     * 清除锁
     * @param string $poolName
     * @param string $key
     */
    private static function delLock(string $poolName, string $key)
    {
        $key = self::getLockKey($key);
        self::getRedis($poolName)->del($key);
    }
    
}