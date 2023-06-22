#pragma once
#include <string>
#include <ostream>
#include <unordered_map>

namespace RPD_Common
{
    typedef enum RedisProtoDataType : int
    {
        RI_None = 0,
        RI_ServiceInfo,              // 服务信息
        RI_ServiceTypeAddrList,      // 服务类型地址列表
        RI_ServiceTypeLevelAddrList, // 服务类型上级地址列表
        RI_ServicePing,              // 服务Ping信息
        RI_ServiceInfoLock,          // 服务信息锁
    } RPD_Type;

    inline std::ostream &operator<<(std::ostream &out, RPD_Type type)
    {
        out << static_cast<std::underlying_type<RPD_Common::RPD_Type>::type>(type);
        return out;
    }

    inline const std::string &GetBasicKey(RPD_Type type) noexcept
    {
        const static std::string empty = "";
        static const std::unordered_map<RPD_Type, std::string> basic_key_list = {
            {RPD_Type::RI_ServiceInfo, "sm_service_info"},
            {RPD_Type::RI_ServiceTypeAddrList, "sm_service_type_addr_list"},
            {RPD_Type::RI_ServiceTypeLevelAddrList, "sm_service_type_level_addr_list"},
            {RPD_Type::RI_ServicePing, "sm_service_ping"},
            {RPD_Type::RI_ServiceInfoLock, "sm_service_info_lock"},
        };

        auto it = basic_key_list.find(type);
        if (it != basic_key_list.end())
        {
            return it->second;
        }
        return empty;
    }

    inline const std::string GetServiceInfoKey(const std::string &group_tab) noexcept
    {
        std::string suffix = "_" + group_tab;
        return GetBasicKey(RPD_Type::RI_ServiceInfo) + suffix;
    }

    inline const std::string GetServiceTypeAddrListKey(const std::string &group_tab, int service_type) noexcept
    {
        std::string suffix = "_" + group_tab;
        suffix += "_" + std::to_string(service_type);
        return GetBasicKey(RPD_Type::RI_ServiceTypeAddrList) + suffix;
    }

    inline const std::string GetServiceTypeLevelAddrListKey(const std::string &group_tab, int service_type) noexcept
    {
        std::string suffix = "_" + group_tab;
        suffix += "_" + std::to_string(service_type);
        return GetBasicKey(RPD_Type::RI_ServiceTypeLevelAddrList) + suffix;
    }

    inline const std::string GetServicePingKey(const std::string &group_tab) noexcept
    {
        std::string suffix = "_" + group_tab;
        return GetBasicKey(RPD_Type::RI_ServicePing) + suffix;
    }

    inline const std::string GetServiceInfoLockKey(const std::string &group_tab, const std::string &addr) noexcept
    {
        std::string ip_port = addr;
        for (size_t idx = 0; idx < ip_port.size(); idx++)
        {
            if (ip_port[idx] == ':' || ip_port[idx] == '.')
            {
                ip_port[idx] = '_';
            }
        }

        std::string suffix = "_" + group_tab + "_" + ip_port;
        return GetBasicKey(RPD_Type::RI_ServiceInfoLock) + suffix;
    }

} // namespace RPD_Common
