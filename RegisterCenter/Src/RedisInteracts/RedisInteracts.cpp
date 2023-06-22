#include "RedisInteracts.h"
#include "glog/logging.h"
#include "TDRedis/TDRedis.h"
#include "TDRedis/TDRedisConnPool.h"

#include "Common/Error.h"
#include "RPD_Common.hpp"

int RedisInteracts::LockServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr,
    const int64_t timeout_ms,
    int64_t &result_value) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "LockServiceInfo() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab和addr拼key
    std::string key = RPD_Common::GetServiceInfoLockKey(group_tab, addr);

    // 发送请求
    TDRedis::Error err = tdRedisPtr->SETNX(key, addr, timeout_ms, result_value);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "LockServiceInfo() SETNX Failed"
                   << ", key = " << key
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::UnlockServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "UnlockServiceInfo() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    int64_t succ = 0;

    // 根据group_tab和addr拼key
    std::string key = RPD_Common::GetServiceInfoLockKey(group_tab, addr);

    // 发送请求
    TDRedis::Error err = tdRedisPtr->UNLINK(key, succ);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "UnlockServiceInfo() UNLINK Failed"
                   << ", key = " << key
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::GetServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr,
    std::string &proto_data) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "GetServiceInfo() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServiceInfoKey(group_tab);

    // 发送请求
    TDRedis::Error err = tdRedisPtr->HGET(key, addr, proto_data);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "GetServiceInfo() HGET Failed"
                   << ", key = " << key
                   << ", addr = " << addr
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::GetMultiServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::vector<std::string> &addr_list,
    std::vector<std::string> &vec_proto_data) noexcept
{
    if (addr_list.size() == 0)
    {
        return Common::Error::OK;
    }

    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "GetMultiServiceInfo() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServiceInfoKey(group_tab);

    // 发送请求
    TDRedis::Error err = tdRedisPtr->HMGET(key, addr_list, vec_proto_data);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "GetMultiServiceInfo() HMGET Failed"
                   << ", key = " << key
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::GetAllServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    std::vector<std::string> &addr_list,
    std::vector<std::string> &vec_proto_data) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "GetAllServiceInfo() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServiceInfoKey(group_tab);

    int64_t cursor = 0;
    std::vector<std::string> tmp_keys;
    std::vector<std::string> tmp_values;
    do
    {
        tmp_values.clear();
        TDRedis::Error err = tdRedisPtr->HSCAN(key, cursor, "", 1000, cursor, tmp_keys, tmp_values);
        if (err != TDRedis::Error::OK)
        {
            LOG(ERROR) << "GetAllServiceInfo() SSCAN Failed"
                       << ", key = " << key
                       << ", cursor = " << cursor
                       << ", err = " << err;
            return Common::Error::RPD_RequestFailed;
        }

        size_t tmp_size = tmp_keys.size();
        for (std::size_t idx = 0; idx < tmp_size; idx++)
        {
            addr_list.push_back(tmp_keys[idx]);
            vec_proto_data.push_back(tmp_values[idx]);
        }
    } while (cursor != 0);

    return Common::Error::OK;
}

int RedisInteracts::SetServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr,
    const std::string &proto_data) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "SetServiceInfo() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServiceInfoKey(group_tab);

    // 发送请求
    TDRedis::Error err = tdRedisPtr->HSET(key, addr, proto_data);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "SetMapInfo() HSET Failed"
                   << ", key = " << key
                   << ", addr = " << addr
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::DelServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "DelServiceInfo() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServiceInfoKey(group_tab);

    // 发送请求
    int64_t retCount = 0;
    TDRedis::Error err = tdRedisPtr->HDEL(key, addr, retCount);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "DelServiceInfo() HDEL Failed"
                   << ", key = " << key
                   << ", addr = " << addr
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::GetAddrListByType(
    const std::string &redis_name,
    const std::string &group_tab,
    const int service_type,
    std::vector<std::string> &addr_list) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "GetAddrListByType() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab和服务类型拼key
    std::string key = RPD_Common::GetServiceTypeAddrListKey(group_tab, service_type);

    int64_t cursor = 0;
    std::vector<std::string> tmp_values;
    do
    {
        tmp_values.clear();
        TDRedis::Error err = tdRedisPtr->SSCAN(key, cursor, "", 1000, cursor, tmp_values);
        if (err != TDRedis::Error::OK)
        {
            LOG(ERROR) << "GetAddrListByType() SSCAN Failed"
                       << ", key = " << key
                       << ", cursor = " << cursor
                       << ", err = " << err;
            return Common::Error::RPD_RequestFailed;
        }

        size_t tmp_size = tmp_values.size();
        for (std::size_t idx = 0; idx < tmp_size; idx++)
        {
            addr_list.push_back(tmp_values[idx]);
        }
    } while (cursor != 0);

    return Common::Error::OK;
}

int RedisInteracts::SAddToAddrList(
    const std::string &redis_name,
    const std::string &group_tab,
    const int service_type,
    const std::string &addr) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "SAddToAddrList() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab和服务类型拼key
    std::string key = RPD_Common::GetServiceTypeAddrListKey(group_tab, service_type);

    // 更新服务列表
    TDRedis::Error err = tdRedisPtr->SADD(key, addr);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "SAddToAddrList() SADD Failed"
                   << ", key = " << key
                   << ", addr = " << addr
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::SRemFromAddrList(
    const std::string &redis_name,
    const std::string &group_tab,
    const int service_type,
    const std::string &addr) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "SRemFromAddrList() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab和服务类型拼key
    std::string key = RPD_Common::GetServiceTypeAddrListKey(group_tab, service_type);

    // 移除地址
    TDRedis::Error err = tdRedisPtr->SREM(key, addr);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "SRemFromAddrList() SREM Failed"
                   << ", key = " << key
                   << ", addr = " << addr
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::GetLevelAddrListByType(
    const std::string &redis_name,
    const std::string &group_tab,
    const int service_type,
    std::vector<std::string> &addr_list) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "GetLevelAddrListByType() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab和服务类型拼key
    std::string key = RPD_Common::GetServiceTypeLevelAddrListKey(group_tab, service_type);

    int64_t cursor = 0;
    std::vector<std::string> tmp_values;
    do
    {
        tmp_values.clear();
        TDRedis::Error err = tdRedisPtr->SSCAN(key, cursor, "", 1000, cursor, tmp_values);
        if (err != TDRedis::Error::OK)
        {
            LOG(ERROR) << "GetLevelAddrListByType() SSCAN Failed"
                       << ", key = " << key
                       << ", cursor = " << cursor
                       << ", err = " << err;
            return Common::Error::RPD_RequestFailed;
        }

        size_t tmp_size = tmp_values.size();
        for (std::size_t idx = 0; idx < tmp_size; idx++)
        {
            addr_list.push_back(tmp_values[idx]);
        }
    } while (cursor != 0);

    return Common::Error::OK;
}

int RedisInteracts::SAddToLevelAddrList(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr,
    const std::vector<int> &vec_rely_service_type) noexcept
{
    if (vec_rely_service_type.size() == 0)
    {
        return Common::Error::OK;
    }

    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "SAddToLevelAddrList() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    for (int rely_service_type : vec_rely_service_type)
    {
        // 根据group_tab和服务类型拼key
        std::string key = RPD_Common::GetServiceTypeLevelAddrListKey(group_tab, rely_service_type);

        // 更新服务列表
        TDRedis::Error err = tdRedisPtr->SADD(key, addr);
        if (err != TDRedis::Error::OK)
        {
            LOG(ERROR) << "SAddToLevelAddrList() SADD Failed"
                       << ", key = " << key
                       << ", addr = " << addr
                       << ", err = " << err;
            return Common::Error::RPD_RequestFailed;
        }
    }

    return Common::Error::OK;
}

int RedisInteracts::SRemFromLevelAddrList(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr,
    const std::vector<int> &vec_rely_service_type) noexcept
{
    if (vec_rely_service_type.size() == 0)
    {
        return Common::Error::OK;
    }

    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "SRemFromLevelAddrList() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    for (int rely_service_type : vec_rely_service_type)
    {
        // 根据group_tab和服务类型拼key
        std::string key = RPD_Common::GetServiceTypeLevelAddrListKey(group_tab, rely_service_type);

        // 移除地址
        TDRedis::Error err = tdRedisPtr->SREM(key, addr);
        if (err != TDRedis::Error::OK)
        {
            LOG(ERROR) << "SRemFromLevelAddrList() SREM Failed"
                       << ", key = " << key
                       << ", addr = " << addr
                       << ", err = " << err;
            return Common::Error::RPD_RequestFailed;
        }
    }

    return Common::Error::OK;
}

int RedisInteracts::GetTimeoutPingAddrList(
    const std::string &redis_name,
    const std::string &group_tab,
    const int64_t timeout,
    std::vector<std::string> &addr_list) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "GetTimeoutPingAddrList() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServicePingKey(group_tab);

    std::vector<std::string> tmp_values;
    TDRedis::Error err = tdRedisPtr->ZRANGEBYSCORE(key, 0, (double)timeout, tmp_values);
    if (TDRedis::Error::OK != err)
    {
        LOG(ERROR) << "GetTimeoutPingAddrList() ZREVRANGEBYSCORE Failed"
                   << ", key = " << key
                   << ", err= " << err;
        return Common::Error::RPD_RequestFailed;
    }

    size_t tmp_size = tmp_values.size();
    addr_list.reserve(tmp_size);
    for (std::size_t idx = 0; idx < tmp_size; idx++)
    {
        addr_list.push_back(tmp_values[idx]);
    }

    return Common::Error::OK;
}

int RedisInteracts::ZAddToPingAddrList(
    const std::string &redis_name,
    const std::string &group_tab,
    const int64_t nowTime,
    const std::string &addr) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "ZAddToPingAddrList() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServicePingKey(group_tab);

    // 更新服务列表
    int64_t retCount = 0;
    TDRedis::Error err = tdRedisPtr->ZADD(key, (double)nowTime, addr, retCount);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "ZAddToPingAddrList() ZADD Failed"
                   << ", key = " << key
                   << ", addr = " << addr
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}

int RedisInteracts::ZRemFromPingAddrList(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr) noexcept
{
    // 获取Redis指针
    auto tdRedisPtr = TDRedisConnPool::GetInstance()->GetConnect(redis_name);
    if (tdRedisPtr == nullptr)
    {
        LOG(ERROR) << "ZRemFromPingAddrList() GetRedisConnPtr Failed"
                   << ", RedisName = " << redis_name;
        return Common::Error::RPD_GetConnFailed;
    }

    // 根据group_tab拼key
    std::string key = RPD_Common::GetServicePingKey(group_tab);

    // 移除地址
    int64_t retCount = 0;
    TDRedis::Error err = tdRedisPtr->ZREM(key, addr, retCount);
    if (err != TDRedis::Error::OK)
    {
        LOG(ERROR) << "ZRemFromPingAddrList() ZREM Failed"
                   << ", key = " << key
                   << ", addr = " << addr
                   << ", err = " << err;
        return Common::Error::RPD_RequestFailed;
    }

    return Common::Error::OK;
}
