#include "Config.h"
#include "glog/logging.h"
#include "Common/Function.h"

Config::Error Config::LoadJson(const std::string &filename)
{
    std::string data;
    Common::FastReadFile(filename, data, false);
    if (data.empty())
    {
        LOG(ERROR) << "[Config::LoadJson()] read file: " << filename << " empty.";
        return Config::Error::JsonDataEmpty;
    }
    return DecodeJsonData(data);
}

Config::Error Config::DecodeJsonData(const std::string &jsonData)
{
    rapidjson::Document doc;
    if (doc.Parse(jsonData.data()).HasParseError())
    {
        LOG(ERROR) << "DecodeJsonData() Parse jsonData HasParseError. err = " << doc.GetParseError();
        return Config::Error::ParseJsonError;
    }

    // 缓存下标
    int dataIdx = (m_dataIdx + 1) % 2;

    auto cfgErr = DecodeRedisList(doc, dataIdx);
    if (cfgErr != Config::Error::OK)
    {
        return cfgErr;
    }

    cfgErr = DecodeGroupList(doc, dataIdx);
    if (cfgErr != Config::Error::OK)
    {
        return cfgErr;
    }

    cfgErr = DecodeLarkWebHook(doc, dataIdx);
    if (cfgErr != Config::Error::OK)
    {
        return cfgErr;
    }

    cfgErr = DecodeRelyWarning(doc, dataIdx);
    if (cfgErr != Config::Error::OK)
    {
        return cfgErr;
    }

    m_dataIdx = dataIdx;
    return Config::Error::OK;
}

Config::Error Config::DecodeRedisList(rapidjson::Document &doc, int dataIdx)
{
    if (!doc.HasMember("RedisList") || !doc["RedisList"].IsArray())
    {
        LOG(ERROR) << "DecodeRedisConfig() Parse jsonData Not Find RedisList.";
        return Config::Error::DecodeRedisListError;
    }

    auto tagsRedisList = doc["RedisList"].GetArray();
    for (rapidjson::SizeType idx = 0; idx < tagsRedisList.Size(); ++idx)
    {
        if (!tagsRedisList[idx].IsObject())
        {
            LOG(ERROR) << "DecodeRedisList() Parse Err, redis idx = " << idx;
            continue;
        }

        auto redisIdx = tagsRedisList[idx].GetObject();

        std::string redis_name = "";
        if (redisIdx.HasMember("Name") && redisIdx["Name"].IsString())
        {
            redis_name = redisIdx["Name"].GetString();
        }
        else
        {
            LOG(ERROR) << "DecodeRedisList() Parse jsonData Not Find Redis Name.";
            return Config::Error::DecodeRedisListError;
        }

        RedisConfig &redisConfig = m_data[dataIdx].m_RedisList[redis_name];
        redisConfig.Name = redis_name;

        if (redisIdx.HasMember("IP") && redisIdx["IP"].IsString())
        {
            redisConfig.IP = redisIdx["IP"].GetString();
        }
        else
        {
            LOG(ERROR) << "DecodeRedisList() Parse jsonData Not Find Redis IP.";
            return Config::Error::DecodeRedisListError;
        }

        if (redisIdx.HasMember("Port") && redisIdx["Port"].IsInt())
        {
            redisConfig.Port = redisIdx["Port"].GetInt();
        }
        else
        {
            LOG(ERROR) << "DecodeRedisList() Parse jsonData Not Find Redis Port.";
            return Config::Error::DecodeRedisListError;
        }

        if (redisIdx.HasMember("Crypto") && redisIdx["Crypto"].IsBool())
        {
            redisConfig.Crypto = redisIdx["Crypto"].GetBool();
        }
        else
        {
            LOG(ERROR) << "DecodeRedisList() Parse jsonData Not Find Redis Crypto.";
            return Config::Error::DecodeRedisListError;
        }

        if (redisIdx.HasMember("Password") && redisIdx["Password"].IsString())
        {
            redisConfig.Password = redisIdx["Password"].GetString();
        }
        else
        {
            LOG(ERROR) << "DecodeRedisList() Parse jsonData Not Find Redis Password.";
            return Config::Error::DecodeRedisListError;
        }

        if (redisIdx.HasMember("Index") && redisIdx["Index"].IsInt())
        {
            int redisIndex = redisIdx["Index"].GetInt();
            if (redisIndex < 0)
            {
                LOG(ERROR) << "DecodeRedisList() Parse jsonData, Redis Index is less then Zero.";
                return Config::Error::DecodeRedisListError;
            }
            redisConfig.Index = redisIndex;
        }
        else
        {
            LOG(ERROR) << "DecodeRedisList() Parse jsonData Not Find Redis Index.";
            return Config::Error::DecodeRedisListError;
        }

        if (redisIdx.HasMember("MaxPoolSize") && redisIdx["MaxPoolSize"].IsInt())
        {
            int redisMaxPoolSize = redisIdx["MaxPoolSize"].GetInt();
            if (redisMaxPoolSize < 0)
            {
                LOG(ERROR) << "DecodeRedisList() Parse jsonData, Redis MaxPoolSize is less then Zero.";
                return Config::Error::DecodeRedisListError;
            }
            redisConfig.MaxPoolSize = redisMaxPoolSize;
        }
        else
        {
            LOG(ERROR) << "DecodeRedisList() Parse jsonData Not Find Redis MaxPoolSize.";
            return Config::Error::DecodeRedisListError;
        }
    }

    return Config::Error::OK;
}

Config::Error Config::DecodeGroupList(rapidjson::Document &doc, int dataIdx)
{
    if (!doc.HasMember("GroupList") || !doc["GroupList"].IsArray())
    {
        LOG(ERROR) << "DecodeGroupConfig() Parse jsonData Not Find GroupList.";
        return Config::Error::DecodeGroupListError;
    }

    auto tagsGroupList = doc["GroupList"].GetArray();
    for (rapidjson::SizeType idx = 0; idx < tagsGroupList.Size(); ++idx)
    {
        if (!tagsGroupList[idx].IsObject())
        {
            LOG(ERROR) << "DecodeGroupList() Parse Err, group_tab idx = " << idx;
            continue;
        }

        auto group_item = tagsGroupList[idx].GetObject();

        std::string group_tab = "";
        if (group_item.HasMember("Group") && group_item["Group"].IsString())
        {
            group_tab = group_item["Group"].GetString();
        }
        else
        {
            LOG(ERROR) << "DecodeGroupList() Parse jsonData Not Find Group.";
            return Config::Error::DecodeGroupListError;
        }

        std::string redis_name = "";
        if (group_item.HasMember("RedisName") && group_item["RedisName"].IsString())
        {
            redis_name = group_item["RedisName"].GetString();
        }
        else
        {
            LOG(ERROR) << "DecodeGroupList() Parse jsonData Not Find redis_name.";
            return Config::Error::DecodeGroupListError;
        }

        m_data[dataIdx].m_GroupList[group_tab] = redis_name;
    }

    return Config::Error::OK;
}

Config::Error Config::DecodeLarkWebHook(rapidjson::Document &doc, int dataIdx)
{
    if (doc.HasMember("LarkWebHook") && doc["LarkWebHook"].IsString())
    {
        m_data[dataIdx].m_LarkWebHook = doc["LarkWebHook"].GetString();
    }
    else
    {
        LOG(ERROR) << "DecodeLarkWebHook() Parse jsonData Not Find LarkWebHook.";
        return Config::Error::DecodeLarkWebHookError;
    }
    return Config::Error::OK;
}

Config::Error Config::DecodeRelyWarning(rapidjson::Document &doc, int dataIdx)
{
    if (doc.HasMember("RelyWarningSwitch") && doc["RelyWarningSwitch"].IsBool())
    {
        m_data[dataIdx].m_RelyWarningSwitch = doc["RelyWarningSwitch"].GetBool();
    }
    else
    {
        LOG(ERROR) << "DecodeRelyWarning() Parse jsonData Not Find RelyWarningSwitch.";
        return Config::Error::DecodeRelyWarningError;
    }

    return Config::Error::OK;
}
