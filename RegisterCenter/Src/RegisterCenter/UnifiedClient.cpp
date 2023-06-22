#include "UnifiedClient.h"
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "Common/Function.h"
#include "LarkMessage/LarkSender.h"
#include "semver/semver.hpp"

#include "TDRedis/TDRedis.h"
#include "TDRedis/TDRedisConnPool.h"

#include "RedisInteracts/RedisInteracts.h"

#include "Config.h"
#include "Define.h"

int64_t UnifiedClient::m_MaxDisconnectTime = 9000;

bool UnifiedClient::Init(
    std::shared_ptr<CTimer> timer,
    const std::string &smAddr)
{
    m_SMAddr = smAddr;

    const auto group_redis_list = Config::GetInstance()->GetGroupList();
    for (auto &item : group_redis_list)
    {
        std::string group_tab = item.first;
        std::string redis_name = item.second;

        std::vector<std::string> addr_list;
        std::vector<std::string> vec_proto_data;
        int ret_err = Common::Error::OK;
        ret_err = RedisInteracts::GetAllServiceInfo(redis_name, group_tab, addr_list, vec_proto_data);
        if (ret_err != Common::Error::OK)
        {
            LOG(ERROR) << "UnifiedClient::Init() Get All ServiceInfo Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", err = " << ret_err;
            return false;
        }

        for (std::string &proto_data : vec_proto_data)
        {
            GrpcProtos::ServiceInfo service_info;
            service_info.ParseFromString(proto_data);
            if ((service_info.status() == GrpcProtos::ServiceStatus::Online ||
                 service_info.status() == GrpcProtos::ServiceStatus::Register) &&
                !service_info.addr().empty())
            {
                OpenClient(service_info.addr(), service_info.service_weight(), service_info.connect_mode());
            }
        }
    }

    m_Timer = timer;
    m_Timer->SetInterval(OnPingMonitorTimer, this, 0, 3000);
    m_Timer->SetInterval(OnChanageNotifyTimer, this, 0, 10);
    m_Timer->SetInterval(OnRelyMonitorTimer, this, 0, 60000);
    m_Timer->SetInterval(OnLarkNotifyTimer, this, 0, 10);

    return true;
}

bool UnifiedClient::LockServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr) noexcept
{
    constexpr int timeout_ms = 50; // 锁超时时间 ms
    constexpr int times = 30;      // 尝试次数
    constexpr int sleep_time = 5;  // 等待间隔 ms

    int64_t service_lock = 0;
    int _loop = times;
    while (!service_lock && _loop--)
    {
        int ret_err = RedisInteracts::LockServiceInfo(
            redis_name, group_tab, addr, timeout_ms, service_lock);
        if (ret_err != Common::Error::OK)
        {
            LOG(ERROR) << "LockServiceInfo() LockServiceInfo Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", addr = " << addr
                       << ", err = " << ret_err;
        }

        if (!service_lock)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        }
    }

    if (!service_lock)
    {
        LOG(ERROR) << "LockServiceInfo() LockServiceInfo Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", addr = " << addr;
        return false;
    }

    return true;
}

bool UnifiedClient::UnlockServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr) noexcept
{
    int ret_err = RedisInteracts::UnlockServiceInfo(
        redis_name, group_tab, addr);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "UnlockServiceInfo() UnlockServiceInfo Failed"
                   << ", group_tab = " << group_tab
                   << ", addr = " << addr
                   << ", err = " << ret_err;
        return false;
    }
    return true;
}

bool UnifiedClient::Notify(
    const std::string client_addr,
    const GrpcProtos::ServiceInfo &service_info)
{
    GrpcProtos::NotifyRequest notifyRequest;
    notifyRequest.mutable_service_info()->CopyFrom(service_info);

    std::string requestData = "";
    if (!notifyRequest.SerializeToString(&requestData))
    {
        return false;
    }

    int result = 0;
    std::string response = "";
    if (!Send(client_addr, GrpcProtos::CMD_NOTIFY, requestData, 1000L, result, response))
    {
        LOG(ERROR) << "Notify() Send Failed! result = " << result
                   << ", response = " << response
                   << ", to (" << client_addr
                   << "), notify(" << GenerateServiceInfoLog(service_info) << ")";
        return false;
    }
    else
    {
        std::transform(response.begin(), response.end(), response.begin(), ::tolower);
        if (GrpcProtos::ResultType::OK != result || 0 != response.compare("ok"))
        {
            LOG(ERROR) << "Notify() Failed! result = " << result
                       << ", response = " << response
                       << ", to (" << client_addr
                       << "), notify(" << GenerateServiceInfoLog(service_info) << ")";
            return false;
        }
    }

    return true;
}

bool UnifiedClient::Hello(
    const GrpcProtos::ServiceInfo &service_info)
{
    std::string helloRequest = "The hello is service manager from " + m_SMAddr;

    int result = 0;
    std::string response = "";
    if (!Send(service_info.addr(), GrpcProtos::CMD_HELLO, helloRequest, 1000L, result, response))
    {
        LOG(ERROR) << "Hello() Failed! result = " << result
                   << ", response = " << response
                   << ", to(" << GenerateServiceInfoLog(service_info) << ")";
        return false;
    }
    else
    {
        std::transform(response.begin(), response.end(), response.begin(), ::tolower);
        if (GrpcProtos::ResultType::OK != result || 0 != response.compare("ok"))
        {
            LOG(ERROR) << "Hello() Failed! result = " << result
                       << ", response = " << response
                       << ", to(" << GenerateServiceInfoLog(service_info) << ")";
            return false;
        }
    }

    return true;
}

int UnifiedClient::RefreshService(
    const std::string &redis_name,
    const GrpcProtos::ServiceInfo &service_info,
    bool &is_notify)
{
    std::string addr = service_info.addr();
    std::string group_tab = service_info.group_tab();

    std::string proto_data = "";
    int ret_err = Common::Error::Failed;
    ret_err = RedisInteracts::GetServiceInfo(redis_name, group_tab, addr, proto_data);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "RefreshService() Get Service Info Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", addr = " << addr
                   << ", err = " << ret_err;
        return ret_err;
    }

    GrpcProtos::ServiceInfo object_service_info;
    if (!proto_data.empty())
    {
        if (!object_service_info.ParseFromString(proto_data))
        {
            LOG(WARNING) << "RefreshService() ParseFromString Failed"
                         << ", redis_name = " << redis_name
                         << ", group_tab = " << group_tab
                         << ", addr = " << addr;
        }
    }

    bool is_clean_service_type_addr = false;
    bool is_clean_service_type_level_addr = false;
    bool is_clean_service_info = false;
    bool is_change_service_type_addr = false;
    bool is_change_service_type_level_addr = false;
    bool is_change_service_info = false;

    if (service_info.semver() != object_service_info.semver() ||
        service_info.service_weight() != object_service_info.service_weight() ||
        service_info.connect_mode() != object_service_info.connect_mode() ||
        service_info.nickname() != object_service_info.nickname() ||
        service_info.service_name() != object_service_info.service_name())
    {
        is_change_service_info = true;
    }

    if (service_info.group_tab() != object_service_info.group_tab())
    {
        is_clean_service_info = true;
        is_change_service_info = true;
    }

    if (service_info.service_type() != object_service_info.service_type())
    {
        if (object_service_info.service_type() != 0)
        {
            is_clean_service_type_addr = true;
        }
        is_change_service_type_addr = true;
        is_change_service_info = true;
    }

    if (service_info.status() != object_service_info.status())
    {
        if (service_info.status() != GrpcProtos::ServiceStatus::Register)
        {
            is_notify = true;
        }

        is_change_service_info = true;
    }

    if (service_info.rely_list_size() != object_service_info.rely_list_size())
    {
        if (object_service_info.rely_list_size() != 0)
        {
            is_clean_service_type_level_addr = true;
        }
        is_change_service_type_level_addr = true;
        is_change_service_info = true;
    }
    else
    {
        std::unordered_map<int, std::string> object_rely_info;
        for (auto &rely_info : object_service_info.rely_list())
        {
            object_rely_info.insert(
                std::make_pair(rely_info.rely_service_type(), rely_info.rely_semver()));
        }

        for (auto &rely_info : service_info.rely_list())
        {
            int rely_service_type = rely_info.rely_service_type();
            auto iter = object_rely_info.find(rely_service_type);
            if (iter == object_rely_info.end())
            {
                is_clean_service_type_level_addr = true;
                is_change_service_type_level_addr = true;
                is_change_service_info = true;
            }
            else
            {
                if (rely_info.rely_semver() != iter->second)
                {
                    is_clean_service_type_level_addr = true;
                    is_change_service_type_level_addr = true;
                    is_change_service_info = true;
                }
            }
        }
    }

    if (is_clean_service_type_addr ||
        is_clean_service_type_level_addr ||
        is_clean_service_info ||
        is_change_service_type_addr ||
        is_change_service_type_level_addr ||
        is_change_service_info)
    {
        if (!LockServiceInfo(redis_name, group_tab, addr))
        {
            return Common::Error::Failed;
        }

        if (is_clean_service_type_addr)
        {
            ret_err = RedisInteracts::SRemFromAddrList(
                redis_name, group_tab, object_service_info.service_type(), addr);
            if (ret_err != Common::Error::OK)
            {
                LOG(ERROR) << "RefreshService() SRem From AddrList Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr
                           << ", service_type = " << object_service_info.service_type()
                           << ", err = " << ret_err;
                return ret_err;
            }
        }

        if (is_clean_service_type_level_addr)
        {
            std::vector<int> vec_rely_service_type;
            for (auto &rely_info : object_service_info.rely_list())
            {
                vec_rely_service_type.push_back(rely_info.rely_service_type());
            }

            ret_err = RedisInteracts::SRemFromLevelAddrList(
                redis_name, group_tab, addr, vec_rely_service_type);
            if (ret_err != Common::Error::OK)
            {
                LOG(ERROR) << "RefreshService() SRem From LevelAddrList Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr
                           << ", err = " << ret_err;
                return ret_err;
            }
        }

        if (is_clean_service_info)
        {
            ret_err = RedisInteracts::DelServiceInfo(
                redis_name, group_tab, addr);
            if (ret_err != Common::Error::OK)
            {
                LOG(ERROR) << "RefreshService() Del ServiceInfo Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr
                           << ", err = " << ret_err;
                return ret_err;
            }
        }

        if (is_change_service_type_addr)
        {
            ret_err = RedisInteracts::SAddToAddrList(
                redis_name, group_tab, service_info.service_type(), addr);
            if (ret_err != Common::Error::OK)
            {
                LOG(ERROR) << "RefreshService() SAdd To AddrList Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr
                           << ", service_type = " << service_info.service_type()
                           << ", err = " << ret_err;
                return ret_err;
            }
        }

        if (is_change_service_type_level_addr)
        {
            std::vector<int> vec_rely_service_type;
            for (auto &rely_info : service_info.rely_list())
            {
                vec_rely_service_type.push_back(rely_info.rely_service_type());
            }

            ret_err = RedisInteracts::SAddToLevelAddrList(
                redis_name, group_tab, addr, vec_rely_service_type);
            if (ret_err != Common::Error::OK)
            {
                LOG(ERROR) << "RefreshService() SAdd To LevelAddrList Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr
                           << ", err = " << ret_err;
                return ret_err;
            }
        }

        if (is_change_service_info)
        {
            std::string new_proto_data = "";
            if (!service_info.SerializeToString(&new_proto_data))
            {
                LOG(ERROR) << "RefreshService() SerializeToString Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr;
                return Common::Error::Failed;
            }

            ret_err = RedisInteracts::SetServiceInfo(redis_name, group_tab, addr, new_proto_data);
            if (ret_err != Common::Error::OK)
            {
                LOG(ERROR) << "RefreshService() Set Service Info Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr
                           << ", err = " << ret_err;
                return ret_err;
            }
        }

        UnlockServiceInfo(redis_name, group_tab, addr);

        OpenClient(addr, service_info.service_weight(), service_info.connect_mode());
    }

    int64_t nowTime = Common::get_ms_timestamp();
    ret_err = RedisInteracts::ZAddToPingAddrList(
        redis_name, group_tab, nowTime, addr);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "RefreshService() ZAdd To PingAddrList Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", nowTime = " << nowTime
                   << ", addr = " << addr
                   << ", err = " << ret_err;
        return ret_err;
    }

    return Common::Error::OK;
}

int UnifiedClient::GetMultiServiceList(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::vector<int> &vec_service_type,
    std::unordered_map<int, std::vector<std::string>> &service_type_list)
{
    if (vec_service_type.size() == 0)
    {
        return Common::Error::OK;
    }

    int ret_err = Common::Error::OK;
    for (int service_type : vec_service_type)
    {
        std::vector<std::string> addr_list;
        ret_err = RedisInteracts::GetAddrListByType(redis_name, group_tab, service_type, addr_list);
        if (ret_err != Common::Error::OK)
        {
            LOG(ERROR) << "GetMultiServiceList() Get AddrList ByType Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", service_type = " << service_type
                       << ", err = " << ret_err;
            return ret_err;
        }

        auto &service_list = service_type_list[service_type];
        service_list.reserve(addr_list.size());

        ret_err = RedisInteracts::GetMultiServiceInfo(redis_name, group_tab, addr_list, service_list);
        if (ret_err != Common::Error::OK)
        {
            LOG(ERROR) << "GetMultiServiceList() Get Multi ServiceInfo Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", service_type = " << service_type
                       << ", err = " << ret_err;
            return ret_err;
        }
    }

    return Common::Error::OK;
}

int UnifiedClient::CheckWatchServiceInfo(
    const std::string &redis_name,
    const std::string &group_tab,
    const GrpcProtos::WatchServiceInfo &watch_serveice_info,
    std::vector<std::string> &object_service_list,
    bool &is_passed)
{
    int service_type = watch_serveice_info.service_type();

    int ret_err = Common::Error::OK;
    std::vector<std::string> addr_list;
    ret_err = RedisInteracts::GetAddrListByType(redis_name, group_tab, service_type, addr_list);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "CheckWatchServiceInfo() Get AddrList ByType Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", service_type = " << service_type
                   << ", err = " << ret_err;
        return ret_err;
    }

    object_service_list.reserve(addr_list.size());
    ret_err = RedisInteracts::GetMultiServiceInfo(redis_name, group_tab, addr_list, object_service_list);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "CheckWatchServiceInfo() Get Multi ServiceInfo Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", service_type = " << service_type
                   << ", err = " << ret_err;
        return ret_err;
    }

    int object_register_online_size = 0;
    std::unordered_map<std::string, GrpcProtos::ServiceInfo> check_object_service_list;
    size_t cache_size = addr_list.size();
    for (size_t idx = 0; idx < cache_size; idx++)
    {
        GrpcProtos::ServiceInfo service_info;
        service_info.ParseFromString(object_service_list[idx]);
        if (service_info.status() == GrpcProtos::ServiceStatus::Online ||
            service_info.status() == GrpcProtos::ServiceStatus::Register)
        {
            check_object_service_list[addr_list[idx]] = service_info;
            object_register_online_size++;
        }
    }

    int register_offline_size = 0;
    for (auto &service_info : watch_serveice_info.service_list())
    {
        if (service_info.status() == GrpcProtos::ServiceStatus::Online ||
            service_info.status() == GrpcProtos::ServiceStatus::Register)
        {
            auto iter = check_object_service_list.find(service_info.addr());
            if (iter == check_object_service_list.end())
            {
                is_passed = false;
                return Common::Error::OK;
            }

            if (iter->second.status() != service_info.status())
            {
                is_passed = false;
                return Common::Error::OK;
            }

            if (iter->second.semver() != service_info.semver())
            {
                is_passed = false;
                return Common::Error::OK;
            }

            if (iter->second.service_weight() != service_info.service_weight())
            {
                is_passed = false;
                return Common::Error::OK;
            }

            if (iter->second.connect_mode() != service_info.connect_mode())
            {
                is_passed = false;
                return Common::Error::OK;
            }

            if (iter->second.group_tab() != service_info.group_tab())
            {
                is_passed = false;
                return Common::Error::OK;
            }

            register_offline_size++;
        }
    }

    if (register_offline_size != object_register_online_size)
    {
        is_passed = false;
        return Common::Error::OK;
    }

    return Common::Error::OK;
}

int UnifiedClient::KictOutService(
    const std::string &redis_name,
    const std::string &group_tab,
    const std::string &addr,
    GrpcProtos::ServiceInfo &service_info,
    bool &isNotify)
{
    if (!LockServiceInfo(redis_name, group_tab, addr))
    {
        return Common::Error::Failed;
    }

    std::string proto_data = "";
    int ret_err = Common::Error::OK;
    ret_err = RedisInteracts::GetServiceInfo(redis_name, group_tab, addr, proto_data);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "KictOutService() Get Service Info Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", addr = " << addr
                   << ", err = " << ret_err;
        return ret_err;
    }

    if (proto_data.empty())
    {
        LOG(WARNING) << "KictOutService() Service Proto Data Is Empty"
                     << ", redis_name = " << redis_name
                     << ", group_tab = " << group_tab
                     << ", addr = " << addr;
        return Common::Error::OK;
    }

    GrpcProtos::ServiceInfo redisServiceInfo;
    if (!redisServiceInfo.ParseFromString(proto_data))
    {
        LOG(ERROR) << "KictOutService() Parse Redis Proto Data Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", addr = " << addr;
        return Common::Error::Failed;
    }

    if (redisServiceInfo.status() == GrpcProtos::ServiceStatus::Online ||
        redisServiceInfo.status() == GrpcProtos::ServiceStatus::Register)
    {
        service_info.CopyFrom(redisServiceInfo);
        service_info.set_status(GrpcProtos::Offline);

        std::string newProtoData = "";
        if (!service_info.SerializeToString(&newProtoData))
        {
            LOG(ERROR) << "KictOutService() Serialize Proto Data Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", addr = " << addr;
            return Common::Error::Failed;
        }

        ret_err = RedisInteracts::SetServiceInfo(redis_name, group_tab, addr, newProtoData);
        if (ret_err != Common::Error::OK)
        {
            LOG(ERROR) << "KictOutService() Set Service Info Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", addr = " << addr
                       << ", err = " << ret_err;
            return ret_err;
        }

        isNotify = true;
    }
    else
    {
        isNotify = false;
    }

    UnlockServiceInfo(redis_name, group_tab, addr);

    return Common::Error::OK;
}

std::string UnifiedClient::GenerateServiceInfoLog(
    const GrpcProtos::ServiceInfo &service_info)
{
    std::stringstream os;
    os << "addr = " << service_info.addr();
    os << ", host_name = " << service_info.host_name();
    os << ", status = " << service_info.status();
    os << ", service_type = " << service_info.service_type();
    os << ", semver = " << service_info.semver();
    os << ", service_weight = " << service_info.service_weight();
    os << ", connect_mode = " << service_info.connect_mode();
    os << ", group_tab = " << service_info.group_tab();
    os << ", service_name = " << service_info.service_name();
    os << ", nickname = " << service_info.nickname();

    for (int idx = 0; idx < service_info.rely_list_size(); idx++)
    {
        if (idx == 0)
        {
            os << ", rely_list = [" << service_info.rely_list(idx).rely_service_type();
        }
        else
        {
            os << ", " << service_info.rely_list(idx).rely_service_type();
        }
        os << ":" << service_info.rely_list(idx).rely_semver();

        if (idx == (service_info.rely_list_size() - 1))
        {
            os << "]";
        }
    }

    return os.str();
}

std::string UnifiedClient::GenerateWatchServiceInfoLog(
    const google::protobuf::RepeatedPtrField<GrpcProtos::WatchServiceInfo> &watch_service_list)
{
    std::stringstream os;
    for (auto &Watch_service_info : watch_service_list)
    {
        if (os.str().empty())
        {
            os << "[service_type = " << Watch_service_info.service_type();
        }
        else
        {
            os << ", [service_type = " << Watch_service_info.service_type();
        }

        for (int idx = 0; idx < Watch_service_info.service_list_size(); idx++)
        {
            if (idx == 0)
            {
                os << ", service_info = [" << GenerateServiceInfoLog(Watch_service_info.service_list(idx));
            }
            else
            {
                os << " | " << GenerateServiceInfoLog(Watch_service_info.service_list(idx));
            }

            if (idx == (Watch_service_info.service_list_size() - 1))
            {
                os << "]";
            }
        }
        os << "]";
    }

    return os.str();
}

int UnifiedClient::PingMonitor(
    const int64_t timeout)
{
    const auto &group_tabList = Config::GetInstance()->GetGroupList();
    for (auto &item : group_tabList)
    {
        std::string group_tab = item.first;
        std::string redis_name = item.second;

        std::vector<std::string> addr_list;
        int ret_err = Common::Error::OK;
        ret_err = RedisInteracts::GetTimeoutPingAddrList(redis_name, group_tab, timeout, addr_list);
        if (ret_err != Common::Error::OK)
        {
            LOG(ERROR) << "PingMonitor() Get Timeout PingAddrList Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", timeout = " << timeout
                       << ", err = " << ret_err;
            return ret_err;
        }

        for (std::string &addr : addr_list)
        {
            GrpcProtos::ServiceInfo service_info;
            bool is_notify;
            ret_err = KictOutService(redis_name, group_tab, addr, service_info, is_notify);
            if (ret_err != Common::Error::OK)
            {
                LOG(ERROR) << "PingMonitor() Get KictOut Service Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab
                           << ", addr = " << addr
                           << ", err = " << ret_err;
                return ret_err;
            }

            if (is_notify)
            {
                PushChangeNotifyQueue(redis_name, service_info);
            }
        }
    }

    return Common::Error::OK;
}

int UnifiedClient::PushChangeNotifyQueue(
    const std::string &redis_name,
    const GrpcProtos::ServiceInfo &service_info)
{
    ChangeNotifyInfo notify_info;
    notify_info.redis_name = redis_name;
    notify_info.service_info.CopyFrom(service_info);
    m_ChangeQueue.push_back(notify_info);

    return Common::Error::OK;
}

int UnifiedClient::ChangeNotifyQueueData()
{
    if (m_ChangeQueue.size() > 0)
    {
        std::vector<ChangeNotifyInfo> vec_notify_info;
        if (m_ChangeQueue.pop_front(vec_notify_info, 10, 0, false))
        {
            for (const auto &notify_info : vec_notify_info)
            {
                ChangeNotify(notify_info.redis_name, notify_info.service_info);
            }
        }
    }

    return Common::Error::OK;
}

int UnifiedClient::ChangeNotify(
    const std::string &redis_name,
    const GrpcProtos::ServiceInfo &service_info)
{
    int service_type = service_info.service_type();
    std::string group_tab = service_info.group_tab();
    std::string addr = service_info.addr();

    int ret_err = Common::Error::OK;
    std::vector<std::string> addr_list;
    ret_err = RedisInteracts::GetLevelAddrListByType(redis_name, group_tab, service_type, addr_list);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "ChangeNotify() Get Level AddrList ByType Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", service_type = " << service_type
                   << ", err = " << ret_err;
        return ret_err;
    }

    std::vector<std::string> vec_proto_data;
    vec_proto_data.reserve(addr_list.size());
    ret_err = RedisInteracts::GetMultiServiceInfo(redis_name, group_tab, addr_list, vec_proto_data);
    if (ret_err != Common::Error::OK)
    {
        LOG(ERROR) << "ChangeNotify() Get Multi ServiceInfo Failed"
                   << ", redis_name = " << redis_name
                   << ", group_tab = " << group_tab
                   << ", service_type = " << service_type
                   << ", err = " << ret_err;
        return ret_err;
    }

    std::vector<GrpcProtos::ServiceInfo> vec_target_service;
    vec_target_service.reserve(vec_proto_data.size());
    for (std::string proto_data : vec_proto_data)
    {
        if (!proto_data.empty())
        {
            GrpcProtos::ServiceInfo target_service;
            target_service.ParseFromString(proto_data);
            vec_target_service.push_back(target_service);
        }
    }

    if (vec_target_service.size() > 0 &&
        service_info.connect_mode() == GrpcProtos::ConnectMode::GRPC &&
        (service_info.status() == GrpcProtos::ServiceStatus::Online ||
         service_info.status() == GrpcProtos::ServiceStatus::Register))
    {
        if (!Hello(service_info))
        {
            GrpcProtos::ServiceInfo ktServiceInfo;
            bool isNotify;
            KictOutService(redis_name, group_tab, addr, ktServiceInfo, isNotify);
            return false;
        }
#ifdef DEBUG
        LOG(INFO) << "[DEBUG] ChangeNotify() Hello change service Succ. change service info(" << GenerateServiceInfoLog(service_info) << ")";
#endif
    }

    for (auto &target_service : vec_target_service)
    {
        if (target_service.status() == GrpcProtos::ServiceStatus::Online ||
            target_service.status() == GrpcProtos::ServiceStatus::Register)
        {
            Hello(target_service);
#ifdef DEBUG
            LOG(INFO) << "[DEBUG] ChangeNotify() Hello target service Succ. target service info(" << GenerateServiceInfoLog(target_service) << ")";
#endif

            if (!Notify(target_service.addr(), service_info))
            {
                LOG(ERROR) << "ChangeNotify() Notify Failed. target info(" << GenerateServiceInfoLog(target_service)
                           << "), notify info(" << GenerateServiceInfoLog(service_info) << ")";
                return false;
            }
#ifdef DEBUG
            else
            {
                LOG(INFO) << "[DEBUG] ChangeNotify() Notify Succ. target info(" << GenerateServiceInfoLog(target_service)
                          << "), notify info(" << GenerateServiceInfoLog(service_info) << ")";
            }
#endif
        }
    }

    return Common::Error::OK;
}

int UnifiedClient::RelyMonitor()
{
    auto &larkWebHook = Config::GetInstance()->GetLarkWebHook();
    auto rely_warning_switch = Config::GetInstance()->GetRelyWarningSwitch();

    const auto &group_tab_List = Config::GetInstance()->GetGroupList();
    for (auto &item : group_tab_List)
    {
        std::string group_tab = item.first;
        std::string redis_name = item.second;

        std::vector<std::string> addr_list;
        std::vector<std::string> vec_proto_data;
        int ret_err = Common::Error::OK;
        ret_err = RedisInteracts::GetAllServiceInfo(redis_name, group_tab, addr_list, vec_proto_data);
        if (ret_err != Common::Error::OK)
        {
            LOG(ERROR) << "RelyMonitor() Get All ServiceInfo Failed"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab
                       << ", err = " << ret_err;
            return ret_err;
        }

        size_t addr_size = addr_list.size();
        if (vec_proto_data.size() != addr_size)
        {
            LOG(ERROR) << "RelyMonitor() All ServiceInfo Value Error"
                       << ", redis_name = " << redis_name
                       << ", group_tab = " << group_tab;
            return Common::Error::Failed;
        }

        std::unordered_map<std::string, std::unordered_set<std::string>> service_level_type_semver_set;
        std::unordered_map<int, std::vector<GrpcProtos::ServiceInfo>> service_type_info;

        for (const auto &proto_data : vec_proto_data)
        {
            GrpcProtos::ServiceInfo service_info;
            if (!service_info.ParseFromString(proto_data))
            {
                LOG(ERROR) << "RelyMonitor() Parse Redis Proto Data Failed"
                           << ", redis_name = " << redis_name
                           << ", group_tab = " << group_tab;
                return Common::Error::Failed;
            }

            std::string level_service_type_semver = std::to_string(service_info.service_type()) + "-" + service_info.semver();
            if (service_info.status() == GrpcProtos::ServiceStatus::Online)
            {
                for (const auto &rely_info : service_info.rely_list())
                {
                    std::string service_type_semver = std::to_string(rely_info.rely_service_type()) + "-" + rely_info.rely_semver();
                    service_level_type_semver_set[service_type_semver].insert(level_service_type_semver);
                }
            }

            service_type_info[service_info.service_type()].push_back(service_info);
        }

        for (const auto &service_level_item : service_level_type_semver_set)
        {
            const auto &service_type_semver = service_level_item.first;
            const auto &level_type_semver_set = service_level_item.second;

            const int service_type = atoi(service_type_semver.substr(0, service_type_semver.find_first_of("-")).c_str());
            const std::string semver = service_type_semver.substr(service_type_semver.find_first_of("-") + 1);

            auto iter = service_type_info.find(service_type);
            if (iter == service_type_info.end())
            {
                continue;
            }

            if (iter->second.size() <= 0)
            {
                continue;
            }

            int online_count = 0;
            for (const auto &service_info : iter->second)
            {
                if (service_info.status() == GrpcProtos::ServiceStatus::Online)
                {
                    std::optional<semver::version> rely_sv = semver::from_string_noexcept(semver);
                    std::optional<semver::version> req_sv = semver::from_string_noexcept(service_info.semver());
                    if (rely_sv.has_value() && req_sv.has_value())
                    {
                        if (req_sv >= rely_sv)
                        {
                            online_count++;
                        }
                    }
                }
            }

            // 如果无下级依赖服务，发送告警
            if (online_count == 0)
            {
                size_t last_index = -1;
                GrpcProtos::ServiceInfo last_service_info;
                for (size_t idx = 0; idx < iter->second.size(); idx++)
                {
                    const auto &service_info = iter->second[idx];
                    if (last_index >= 0)
                    {
                        std::optional<semver::version> last_sv = semver::from_string_noexcept(last_service_info.semver());
                        std::optional<semver::version> req_sv = semver::from_string_noexcept(service_info.semver());
                        if (req_sv.has_value())
                        {
                            if (req_sv > last_sv)
                            {
                                last_index = idx;
                                last_service_info.CopyFrom(service_info);
                            }
                        }
                    }
                    else
                    {
                        std::optional<semver::version> req_sv = semver::from_string_noexcept(service_info.semver());
                        if (req_sv.has_value())
                        {
                            last_index = idx;
                            last_service_info.CopyFrom(service_info);
                        }
                    }
                }

                if (last_index >= 0 && last_index < iter->second.size())
                {
                    std::string level_service_info = "";
                    for (const auto &level_info : level_type_semver_set)
                    {
                        if (!level_service_info.empty())
                        {
                            level_service_info.append(", ");
                        }
                        level_service_info.append(level_info);
                    }

                    std::string warning_info = "-服务缺失";
                    if (rely_warning_switch)
                    {
                        Common::NotifyLark_RelyWarning(
                            larkWebHook,
                            last_service_info.nickname() + warning_info,
                            group_tab,
                            std::to_string(last_service_info.service_type()),
                            last_service_info.service_name(),
                            "level_service_info: [" + level_service_info + "]");
                    }

                    LOG(WARNING) << "RelyMonitor() Missing services"
                                 << ", group_tab = " << group_tab
                                 << ", title = " << last_service_info.nickname() + warning_info
                                 << ", service_name = " << last_service_info.service_name()
                                 << ", service_type = " << last_service_info.service_type()
                                 << ", level_service_info = [" << level_service_info << "]";
                }
            }
        }
    }

    return Common::Error::OK;
}

int UnifiedClient::PushLarkNotifyQueue(
    const GrpcProtos::ServiceInfo &service_info)
{
    LarkNotifyInfo notify_info;
    notify_info.service_info.CopyFrom(service_info);
    m_LarkQueue.push_back(notify_info);

    return Common::Error::OK;
}

int UnifiedClient::LarkNotifyQueueData()
{
    if (m_LarkQueue.size() > 0)
    {
        auto &larkWebHook = Config::GetInstance()->GetLarkWebHook();

        std::vector<LarkNotifyInfo> vec_notify_info;
        if (m_LarkQueue.pop_front(vec_notify_info, 10, 0, false))
        {
            for (const auto &notify_info : vec_notify_info)
            {
                std::string change_info = "";
                int service_status = notify_info.service_info.status();
                if (service_status == GrpcProtos::ServiceStatus::Register)
                {
                    change_info = "-服务注册";
                }
                else if (service_status == GrpcProtos::ServiceStatus::Online)
                {
                    change_info = "-服务上线";
                }
                else if (service_status == GrpcProtos::ServiceStatus::Offline)
                {
                    change_info = "-服务下线";
                }

                Common::NotifyLark_Change(
                    larkWebHook,
                    notify_info.service_info.nickname() + change_info,
                    notify_info.service_info.addr(),
                    notify_info.service_info.service_name(),
                    notify_info.service_info.semver(),
                    "");

                LOG(INFO) << "PushLarkNotifyQueue() Lark Notify Info"
                          << ", larkWebHook = " << larkWebHook
                          << ", title = " << notify_info.service_info.nickname() + change_info
                          << ", addr = " << notify_info.service_info.addr()
                          << ", service_name = " << notify_info.service_info.service_name()
                          << ", semver = " << notify_info.service_info.semver();
            }
        }
    }

    return Common::Error::OK;
}

void UnifiedClient::OnPingMonitorTimer(void *obj, int timer_id)
{
    UnifiedClient *client = (UnifiedClient *)obj;

    int64_t nowTime = Common::get_ms_timestamp();
    int64_t timeout = nowTime - m_MaxDisconnectTime;

    client->PingMonitor(timeout);
}

void UnifiedClient::OnChanageNotifyTimer(void *obj, int timer_id)
{
    UnifiedClient *client = (UnifiedClient *)obj;

    client->ChangeNotifyQueueData();
}

void UnifiedClient::OnRelyMonitorTimer(void *obj, int timer_id)
{
    UnifiedClient *client = (UnifiedClient *)obj;

    client->RelyMonitor();
}

void UnifiedClient::OnLarkNotifyTimer(void *obj, int timer_id)
{
    UnifiedClient *client = (UnifiedClient *)obj;

    client->LarkNotifyQueueData();
}
