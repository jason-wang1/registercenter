#pragma once
#include <ostream>
#include <string>
#include <map>
#include <vector>
#include <unordered_set>
#include <unordered_map>

class RedisInteracts
{
public:
    // 服务信息锁
    static int LockServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr,
        const int64_t timeout_ms,
        int64_t &result_value) noexcept;

    static int UnlockServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr) noexcept;

public:
    // 服务信息
    static int GetServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr,
        std::string &proto_data) noexcept;

    static int GetMultiServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::vector<std::string> &addr_list,
        std::vector<std::string> &vec_proto_data) noexcept;

    static int GetAllServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        std::vector<std::string> &addr_list,
        std::vector<std::string> &vec_proto_data) noexcept;

    static int SetServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr,
        const std::string &proto_data) noexcept;

    static int DelServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr) noexcept;

public:
    // 服务类型-->服务列表
    static int GetAddrListByType(
        const std::string &redis_name,
        const std::string &group_tab,
        const int service_type,
        std::vector<std::string> &addr_list) noexcept;

    static int SAddToAddrList(
        const std::string &redis_name,
        const std::string &group_tab,
        const int service_type,
        const std::string &addr) noexcept;

    static int SRemFromAddrList(
        const std::string &redis_name,
        const std::string &group_tab,
        const int service_type,
        const std::string &addr) noexcept;

public:
    // 服务类型-->上级服务列表
    static int GetLevelAddrListByType(
        const std::string &redis_name,
        const std::string &group_tab,
        const int service_type,
        std::vector<std::string> &addr_list) noexcept;

    static int SAddToLevelAddrList(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr,
        const std::vector<int> &vec_rely_service_type) noexcept;

    static int SRemFromLevelAddrList(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr,
        const std::vector<int> &vec_rely_service_type) noexcept;

public:
    // 服务Ping
    static int GetTimeoutPingAddrList(
        const std::string &redis_name,
        const std::string &group_tab,
        const int64_t timeout,
        std::vector<std::string> &addr_list) noexcept;

    static int ZAddToPingAddrList(
        const std::string &redis_name,
        const std::string &group_tab,
        const int64_t nowTime,
        const std::string &addr) noexcept;

    static int ZRemFromPingAddrList(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr) noexcept;
};
