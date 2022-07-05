<?php

if (!function_exists('filterArrKeys')) {
    /**
     * 过滤数组中的key
     * @param $data
     * @param $keys
     * @return array
     */
    function filterArrKeys($data, $keys): array
    {
        if (empty($data) || !is_array($data)) {
            return [];
        }
        return array_filter(
            $data,
            function ($val, $key) use ($keys) {
                # 过滤不需要的键值
                if (in_array($key, $keys)) {
                    return true;
                }
                return false;
            },
            ARRAY_FILTER_USE_BOTH
        );
    }
}