#pragma once
#include <stdint.h>

#include <string>
#include <vector>
#include <unordered_map>

#include "grpcpp/grpcpp.h"

#include "timer/timer.h"
#include "Common/Singleton.h"
#include "ClientManager/GrpcSender.h"
#include "ClientManager/GrpcClient.h"

#include "Common/SafeQueue.h"

struct ChangeNotifyInfo
{
    std::string redis_name = "";
    GrpcProtos::ServiceInfo service_info;
};

struct LarkNotifyInfo
{
    GrpcProtos::ServiceInfo service_info;
};

// 远程服务客户端类
class UnifiedClient
    : public GrpcClient<GrpcSender>,
      public Singleton<UnifiedClient>
{
public:
    UnifiedClient(token) {}
    virtual ~UnifiedClient() {}
    UnifiedClient(const UnifiedClient &) = delete;
    UnifiedClient &operator=(const UnifiedClient &) = delete;

    bool Init(
        std::shared_ptr<CTimer> timer,
        const std::string &smAddr);

    bool LockServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr) noexcept;

    bool UnlockServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr) noexcept;

public:
    // 向客户端推送注册、上线、下线服务信息
    bool Notify(
        const std::string client_addr,
        const GrpcProtos::ServiceInfo &service_info);

    bool Hello(
        const GrpcProtos::ServiceInfo &service_info);

    // 刷新Service
    int RefreshService(
        const std::string &redis_name,
        const GrpcProtos::ServiceInfo &service_info,
        bool &is_notify);

    // 获取服务列表
    int GetMultiServiceList(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::vector<int> &vec_service_type,
        std::unordered_map<int, std::vector<std::string>> &service_type_list);

    // 检查关注服务信息
    int CheckWatchServiceInfo(
        const std::string &redis_name,
        const std::string &group_tab,
        const GrpcProtos::WatchServiceInfo &watch_serveice_info,
        std::vector<std::string> &object_service_list,
        bool &is_passed);

    // 剔出服务
    int KictOutService(
        const std::string &redis_name,
        const std::string &group_tab,
        const std::string &addr,
        GrpcProtos::ServiceInfo &service_info,
        bool &isNotify);

    std::string GenerateServiceInfoLog(
        const GrpcProtos::ServiceInfo &service_info);

    std::string GenerateWatchServiceInfoLog(
        const google::protobuf::RepeatedPtrField<GrpcProtos::WatchServiceInfo> &watch_service_list);

public:
    int PingMonitor(
        const int64_t timeout);

    int PushChangeNotifyQueue(
        const std::string &redis_name,
        const GrpcProtos::ServiceInfo &service_info);

    int ChangeNotifyQueueData();

    int ChangeNotify(
        const std::string &redis_name,
        const GrpcProtos::ServiceInfo &service_info);

    int RelyMonitor();

    int PushLarkNotifyQueue(
        const GrpcProtos::ServiceInfo &service_info);

    int LarkNotifyQueueData();

private:
    std::shared_ptr<CTimer> m_Timer;

    // 最大断开连接时间
    static int64_t m_MaxDisconnectTime;

    // 服务Ping监控
    static void OnPingMonitorTimer(void *obj, int timer_id);

    // 服务动态推送
    static void OnChanageNotifyTimer(void *obj, int timer_id);

    // 服务依赖监控
    static void OnRelyMonitorTimer(void *obj, int timer_id);

    // 消息推送
    static void OnLarkNotifyTimer(void *obj, int timer_id);

private:
    std::string m_SMAddr;

    // 服务动态推送队列
    SafeQueue<ChangeNotifyInfo> m_ChangeQueue;

    // 消息推送队列
    SafeQueue<LarkNotifyInfo> m_LarkQueue;
};
