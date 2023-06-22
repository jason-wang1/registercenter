#include "RegisterCenter.h"
#include <future>
#include <thread>
#include "glog/logging.h" // glog 头文件
#include "Common/Function.h"
#include "GrpcDispatcher/GrpcDispatcher.h"
#include "TDRedis/TDRedis.h"
#include "TDRedis/TDRedisConnPool.h"

#include "Define.h"
#include "Config.h"
#include "Application.h"
#include "UnifiedClient.h"

bool RegisterCenter::Init()
{
    GrpcDispatcher::GetInstance()->Bind(
        this, GrpcProtos::CmdType::CMD_HELLO, OnHello);

    GrpcDispatcher::GetInstance()->Bind(
        this, GrpcProtos::CmdType::CMD_ONLINE, OnOnline);

    GrpcDispatcher::GetInstance()->Bind(
        this, GrpcProtos::CmdType::CMD_REGISTER, OnRegister);

    GrpcDispatcher::GetInstance()->Bind(
        this, GrpcProtos::CmdType::CMD_OFFLINE, OnOffline);

    GrpcDispatcher::GetInstance()->Bind(
        this, GrpcProtos::CmdType::CMD_PING, OnPing);

    GrpcDispatcher::GetInstance()->Bind(
        this, GrpcProtos::CmdType::CMD_CHECK, OnCheck);

    return true;
}

bool RegisterCenter::OnHello(
    std::any obj,
    const int cmd,
    const long deadline_ms,
    const std::string &request,
    int &result, std::string &response)
{
    LOG(INFO) << "OnHello() request = " << request;

    result = GrpcProtos::ResultType::OK;
    response = "ok";

    return true;
}

bool RegisterCenter::OnRegister(
    std::any obj,
    const int cmd,
    const long deadline_ms,
    const std::string &request,
    int &result,
    std::string &response)
{
    do
    {
        GrpcProtos::RegisterRequest protoRequest;
        if (!protoRequest.ParseFromString(request))
        {
            result = GrpcProtos::ResultType::ERR_Decode_Request;
            response = "Parse RegisterRequest Error";
            LOG(ERROR) << "OnRegister() Parse RegisterRequest Error!";
            break;
        }

        GrpcProtos::ServiceInfo service_info = protoRequest.service_info();
        LOG(INFO) << "OnRegister() ServiceInfo(" << UnifiedClient::GetInstance()->GenerateServiceInfoLog(service_info);

        if (service_info.addr().empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnRegister Service Address Is Empty Error";
            LOG(ERROR) << "OnRegister() Service Address Is Empty Error!";
            break;
        }

        std::string group_tab = service_info.group_tab();
        std::string redis_name = Config::GetInstance()->GetRedisName(group_tab);
        if (redis_name.empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnRegister GroupTab Config Error";
            LOG(ERROR) << "OnRegister() Group Config Error, group_tab = " << group_tab;
            break;
        }

        // 刷新服务
        bool is_notify = false;
        int ret_err = Common::Error::OK;
        ret_err = UnifiedClient::GetInstance()->RefreshService(redis_name, service_info, is_notify);
        if (ret_err != Common::Error::OK)
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnRegister Refresh Service Failed";
            LOG(ERROR) << "OnRegister() Refresh Service Failed, err = " << ret_err;
            break;
        }

        std::vector<int> vec_rely_service_type;
        for (auto &rely_info : service_info.rely_list())
        {
            vec_rely_service_type.push_back(rely_info.rely_service_type());
        }

        std::unordered_map<int, std::vector<std::string>> service_type_list;
        ret_err = UnifiedClient::GetInstance()->GetMultiServiceList(
            redis_name, group_tab, vec_rely_service_type, service_type_list);
        if (ret_err != Common::Error::OK)
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnRegister Get Rely Service List Failed";
            LOG(ERROR) << "OnRegister() GetMultiTypeServiceList Failed, err = " << ret_err;
            break;
        }

        // 填充当前服务关注的下级服务列表
        GrpcProtos::RegisterReply protoReply;
        for (auto &item : service_type_list)
        {
            GrpcProtos::ServiceType rely_service_type =
                static_cast<GrpcProtos::ServiceType>(item.first);

            auto watch_service_info_ptr = protoReply.add_watch_list();
            watch_service_info_ptr->set_service_type(rely_service_type);
            for (std::string &proto_data : item.second)
            {
                if (!proto_data.empty())
                {
                    watch_service_info_ptr->add_service_list()->ParseFromString(proto_data);
                }
            }
        }

        if (!protoReply.SerializeToString(&response))
        {
            result = GrpcProtos::ResultType::ERR_Encode_Response;
            response = "Encode OnRegister Reply Error";
            LOG(ERROR) << "OnRegister() Encode Register Reply Error!";
            break;
        }

        result = GrpcProtos::ResultType::OK;

        UnifiedClient::GetInstance()->PushLarkNotifyQueue(protoRequest.service_info());

    } while (false);

    return true;
}

bool RegisterCenter::OnOnline(
    std::any obj,
    const int cmd,
    const long deadline_ms,
    const std::string &request,
    int &result, std::string &response)
{
    do
    {
        GrpcProtos::OnlineRequest protoRequest;
        if (!protoRequest.ParseFromString(request))
        {
            result = GrpcProtos::ResultType::ERR_Decode_Request;
            response = "Parse Online Request Error";
            LOG(ERROR) << "OnOnline() Parse Register Request Error!";
            break;
        }

        GrpcProtos::ServiceInfo service_info = protoRequest.service_info();
        LOG(INFO) << "OnOnline() ServiceInfo(" << UnifiedClient::GetInstance()->GenerateServiceInfoLog(service_info);

        if (service_info.addr().empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnOnline Service Address Is Empty Error";
            LOG(ERROR) << "OnOnline() Service Address Is Empty Error!";
            break;
        }

        std::string group_tab = service_info.group_tab();
        std::string redis_name = Config::GetInstance()->GetRedisName(group_tab);
        if (redis_name.empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnOnline GroupTab Config Error";
            LOG(ERROR) << "OnOnline() Group Config Error, group_tab = " << group_tab;
            break;
        }

        // 刷新服务
        bool is_notify = false;
        int ret_err = Common::Error::OK;
        ret_err = UnifiedClient::GetInstance()->RefreshService(redis_name, service_info, is_notify);
        if (ret_err != Common::Error::OK)
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnOnline Refresh Service Failed";
            LOG(ERROR) << "OnOnline() Refresh Service Failed, err = " << ret_err;
            break;
        }

        std::vector<int> vec_rely_service_type;
        for (auto &rely_info : service_info.rely_list())
        {
            vec_rely_service_type.push_back(rely_info.rely_service_type());
        }

        std::unordered_map<int, std::vector<std::string>> service_type_list;
        ret_err = UnifiedClient::GetInstance()->GetMultiServiceList(
            redis_name, group_tab, vec_rely_service_type, service_type_list);
        if (ret_err != Common::Error::OK)
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnOnline Get Rely Service List Failed";
            LOG(ERROR) << "OnOnline() GetMultiTypeServiceList Failed, err = " << ret_err;
            break;
        }

        // 填充当前服务关注的下级服务列表
        GrpcProtos::OnlineReply protoReply;
        for (auto &item : service_type_list)
        {
            GrpcProtos::ServiceType rely_service_type =
                static_cast<GrpcProtos::ServiceType>(item.first);

            auto watch_service_info_ptr = protoReply.add_watch_list();
            watch_service_info_ptr->set_service_type(rely_service_type);
            for (std::string &proto_data : item.second)
            {
                if (!proto_data.empty())
                {
                    watch_service_info_ptr->add_service_list()->ParseFromString(proto_data);
                }
            }
        }

        if (!protoReply.SerializeToString(&response))
        {
            result = GrpcProtos::ResultType::ERR_Encode_Response;
            response = "Encode Online Reply Error";
            LOG(ERROR) << "OnOnline() Encode Register Reply Error!";
            break;
        }

        result = GrpcProtos::ResultType::OK;

        if (is_notify)
        {
            UnifiedClient::GetInstance()->PushChangeNotifyQueue(redis_name, service_info);
        }

        UnifiedClient::GetInstance()->PushLarkNotifyQueue(protoRequest.service_info());

    } while (false);

    return true;
}

bool RegisterCenter::OnOffline(
    std::any obj,
    const int cmd,
    const long deadline_ms,
    const std::string &request,
    int &result,
    std::string &response)
{
    do
    {
        GrpcProtos::OfflineRequest protoRequest;
        if (!protoRequest.ParseFromString(request))
        {
            result = GrpcProtos::ResultType::ERR_Decode_Request;
            response = "Parse Offline Request Error";
            LOG(ERROR) << "OnOffline() Parse Offline Request Error!";
            break;
        }

        GrpcProtos::ServiceInfo service_info = protoRequest.service_info();
        LOG(INFO) << "OnOffline() ServiceInfo(" << UnifiedClient::GetInstance()->GenerateServiceInfoLog(service_info);

        if (service_info.addr().empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnOffline Service Address Is Empty Error";
            LOG(ERROR) << "OnOffline() Service Address Is Empty Error!";
            break;
        }

        std::string group_tab = service_info.group_tab();
        std::string redis_name = Config::GetInstance()->GetRedisName(group_tab);
        if (redis_name.empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnOffline GroupTab Config Error";
            LOG(ERROR) << "OnOffline() Group Config Error, group_tab = " << group_tab;
            break;
        }

        // 刷新服务
        bool is_notify = false;
        int ret_err = Common::Error::OK;
        ret_err = UnifiedClient::GetInstance()->RefreshService(redis_name, service_info, is_notify);
        if (ret_err != Common::Error::OK)
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnOffline Refresh Service Failed";
            LOG(ERROR) << "OnOffline() Refresh Service Failed, err = " << ret_err;
            break;
        }

        result = GrpcProtos::ResultType::OK;
        response = "ok";

        if (is_notify)
        {
            // 下线同步广播通知上级服务
            UnifiedClient::GetInstance()->ChangeNotify(redis_name, service_info);
        }

        UnifiedClient::GetInstance()->PushLarkNotifyQueue(protoRequest.service_info());

    } while (false);

    return true;
}

bool RegisterCenter::OnPing(
    std::any obj,
    const int cmd,
    const long deadline_ms,
    const std::string &request,
    int &result,
    std::string &response)
{
    do
    {
        GrpcProtos::PingRequest protoRequest;
        if (!protoRequest.ParseFromString(request))
        {
            result = GrpcProtos::ResultType::ERR_Decode_Request;
            response = "Parse Ping Request Error";
            LOG(ERROR) << "OnPing() Parse Ping Request Error!";
            break;
        }

        GrpcProtos::ServiceInfo service_info = protoRequest.service_info();
        LOG(INFO) << "OnPing() ServiceInfo(" << UnifiedClient::GetInstance()->GenerateServiceInfoLog(service_info);

        if (service_info.addr().empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnPing Service Address Is Empty Error";
            LOG(ERROR) << "OnPing() Service Address Is Empty Error!";
            break;
        }

        std::string group_tab = service_info.group_tab();
        std::string redis_name = Config::GetInstance()->GetRedisName(group_tab);
        if (redis_name.empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnPing GroupTab Config Error";
            LOG(ERROR) << "OnPing() Group Config Error, group_tab = " << group_tab;
            break;
        }

        // 刷新服务
        bool is_notify = false;
        int ret_err = Common::Error::OK;
        ret_err = UnifiedClient::GetInstance()->RefreshService(redis_name, service_info, is_notify);
        if (ret_err != Common::Error::OK)
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnPing Refresh Service Failed";
            LOG(ERROR) << "OnPing() Refresh Service Failed, err = " << ret_err;
            break;
        }

        result = GrpcProtos::ResultType::OK;
        response = "ok";

        if (is_notify)
        {
            UnifiedClient::GetInstance()->PushChangeNotifyQueue(redis_name, service_info);
        }

    } while (false);

    return true;
}

bool RegisterCenter::OnCheck(
    std::any obj,
    const int cmd,
    const long deadline_ms,
    const std::string &request,
    int &result,
    std::string &response)
{
    do
    {
        GrpcProtos::CheckRequest protoRequest;
        if (!protoRequest.ParseFromString(request))
        {
            result = GrpcProtos::ResultType::ERR_Decode_Request;
            response = "Parse Check Request Error";
            LOG(ERROR) << "OnCheck() Parse Check Request Error!";
            break;
        }

        GrpcProtos::ServiceInfo service_info = protoRequest.service_info();
        LOG(INFO) << "OnCheck() ServiceInfo(" << UnifiedClient::GetInstance()->GenerateServiceInfoLog(service_info);

        if (service_info.addr().empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnCheck Service Address Is Empty Error";
            LOG(ERROR) << "OnCheck() Service Address Is Empty Error!";
            break;
        }

        std::string group_tab = service_info.group_tab();
        std::string redis_name = Config::GetInstance()->GetRedisName(group_tab);
        if (redis_name.empty())
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnCheck GroupTab Config Error";
            LOG(ERROR) << "OnCheck() Group Config Error, group_tab = " << group_tab;
            break;
        }

        bool is_passed = true;
        int ret_err = Common::Error::OK;
        std::unordered_map<int, std::vector<std::string>> rely_object_service_list;
        for (auto &watch_serveice_info : protoRequest.watch_list())
        {
            int service_type = watch_serveice_info.service_type();
            auto &object_service_list = rely_object_service_list[service_type];
            ret_err = UnifiedClient::GetInstance()->CheckWatchServiceInfo(
                redis_name, group_tab, watch_serveice_info, object_service_list, is_passed);
            if (ret_err != Common::Error::OK)
            {
                break;
            }
        }

        if (ret_err != Common::Error::OK)
        {
            result = GrpcProtos::ResultType::ERR_Call_Service;
            response = "OnCheck Check WatchServiceList Failed";
            LOG(ERROR) << "OnCheck() CheckWatchServiceList Failed, err = " << ret_err;
            break;
        }

        if (is_passed)
        {
            result = GrpcProtos::ResultType::OK;
            response = "ok";
        }
        else
        {
            LOG(WARNING) << "OnCheck() Check failed, ServiceInfo(" << UnifiedClient::GetInstance()->GenerateServiceInfoLog(service_info) << ")";

            // 填充当前服务关注的下级服务列表
            GrpcProtos::CheckReply protoReply;
            for (auto &item : rely_object_service_list)
            {
                GrpcProtos::ServiceType rely_service_type =
                    static_cast<GrpcProtos::ServiceType>(item.first);

                auto watch_service_info_ptr = protoReply.add_watch_list();
                watch_service_info_ptr->set_service_type(rely_service_type);
                for (std::string &proto_data : item.second)
                {
                    if (!proto_data.empty())
                    {
                        watch_service_info_ptr->add_service_list()->ParseFromString(proto_data);
                    }
                }
            }

            if (!protoReply.SerializeToString(&response))
            {
                result = GrpcProtos::ResultType::ERR_Encode_Response;
                response = "Encode Check Reply Error";
                LOG(ERROR) << "OnCheck() Encode Check Reply Error!";
                break;
            }

            result = GrpcProtos::ResultType::OK;
        }

    } while (false);

    return true;
}
