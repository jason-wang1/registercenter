#pragma once
#include <mutex>
#include <memory>
#include <atomic>
#include <vector>
#include <unordered_map>
#include "rapidjson/document.h"
#include "Common/Singleton.h"

struct RedisConfig
{
    std::string Name;
    std::string IP;
    int Port = 0;
    bool Crypto = false;
    std::string Password;
    int Index = 0;
    int MaxPoolSize = 0;
};

struct ConfigData
{
    // Redis配置
    std::unordered_map<std::string, RedisConfig> m_RedisList;

    // Group配置
    std::unordered_map<std::string, std::string> m_GroupList;

    // 飞书地址
    std::string m_LarkWebHook;

    // 依赖服务监控开关
    bool m_RelyWarningSwitch = false;
};

class Config : public Singleton<Config>
{
public:
    typedef enum ConfigErrorCode
    {
        OK = 0,
        JsonDataEmpty = 1,
        ParseJsonError,
        DecodeRedisListError,
        DecodeGroupListError,
        DecodeLarkWebHookError,
        DecodeRelyWarningError,
    } Error;

public:
    Config::Error LoadJson(const std::string &filename);

    const std::unordered_map<std::string, RedisConfig> &GetRedisConfList() const noexcept
    {
        return m_data[m_dataIdx].m_RedisList;
    }

    const std::unordered_map<std::string, std::string> &GetGroupList() const noexcept
    {
        return m_data[m_dataIdx].m_GroupList;
    }

    inline const std::string GetRedisName(const std::string &group_tabTab) const noexcept
    {
        auto iter = m_data[m_dataIdx].m_GroupList.find(group_tabTab);
        if (iter != m_data[m_dataIdx].m_GroupList.end())
        {
            return iter->second;
        }
        return "";
    }

    inline const std::string GetLarkWebHook() const noexcept
    {
        return std::cref(m_data[m_dataIdx].m_LarkWebHook);
    }

    inline bool GetRelyWarningSwitch() const noexcept
    {
        return m_data[m_dataIdx].m_RelyWarningSwitch;
    }

    Config(token) { m_dataIdx = 0; }
    ~Config() {}
    Config(Config &) = delete;
    Config &operator=(const Config &) = delete;

protected:
    Config::Error DecodeJsonData(const std::string &jsonData);
    Config::Error DecodeRedisList(rapidjson::Document &doc, int dataIdx);
    Config::Error DecodeGroupList(rapidjson::Document &doc, int dataIdx);
    Config::Error DecodeLarkWebHook(rapidjson::Document &doc, int dataIdx);
    Config::Error DecodeRelyWarning(rapidjson::Document &doc, int dataIdx);

private:
    mutable std::atomic<int> m_dataIdx;
    ConfigData m_data[2];
};
