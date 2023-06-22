#include "Application.h"
#include <thread>
#include <unordered_map>
#include "glog/logging.h"
#include "timer/timer.h"
#include "GrpcDispatcher/GrpcReceiver.h"
#include "GrpcDispatcher/GrpcDispatcher.h"
#include "TDRedis/TDRedis.h"
#include "TDRedis/TDRedisConnPool.h"

#include "Define.h"
#include "Config.h"
#include "RegisterCenter.h"
#include "UnifiedClient.h"

bool Application::Init(const std::string &IP,
                       const std::string &Port,
                       const std::string &Exe,
                       const std::string &Nicename)
{
    LOG(INFO) << "Application->Init("
              << "IP = " << IP
              << ", Port = " << Port
              << ", Exe = " << Exe
              << ", Nicename = " << Nicename
              << ")";

    // 注册信号
    CSignal::installSignal(SIGINT, notifyShutdown);
    CSignal::installSignal(SIGQUIT, notifyShutdown);
    CSignal::installSignal(SIGTERM, notifyShutdown);
    CSignal::installSignal(SignalNumber::StopProcess, notifyShutdown);

    conf = Config::GetInstance();
    if (Config::Error::OK != conf->LoadJson(Common::ConfigPath_App))
    {
        LOG(ERROR) << "LoadJson Error!! config Path = " << Common::ConfigPath_App;
        return false;
    }

    // 初始化Redis连接池
    const auto redisConfList = conf->GetRedisConfList();
    for (const auto &item : redisConfList)
    {
        auto redis = item.second;
        if (!TDRedisConnPool::GetInstance()->AddConnPool(
                redis.Name, redis.IP, redis.Port,
                redis.Crypto, redis.Password,
                redis.Index, redis.MaxPoolSize))
        {
            LOG(ERROR) << "Init Redis Conn Pool Error"
                       << ", redis.Name = " << redis.Name
                       << ", redis.IP = " << redis.IP
                       << ", redis.Port = " << redis.Port
                       << ", redis.Crypto = " << redis.Crypto
                       << ", redis.Password = " << redis.Password
                       << ", redis.Index = " << redis.Index
                       << ", redis.MaxPoolSize = " << redis.MaxPoolSize;
            return false;
        }
        else
        {
            LOG(INFO) << "Init Redis Conn Pool Succ, redis.Name = " << redis.Name;
        }
    }

    // timer
    timer = std::shared_ptr<CTimer>(new CTimer);

    // UnifiedClient
    std::string smAddr = IP + ":" + Port;
    if (!UnifiedClient::GetInstance()->Init(timer, smAddr))
    {
        LOG(ERROR) << "Init UnifiedClient  Error";
        return false;
    }
    else
    {
        LOG(INFO) << "Init UnifiedClient Succ.";
    }

    // 注册服务接口
    svcManager = std::make_shared<RegisterCenter>();
    if (!svcManager->Init())
    {
        LOG(ERROR) << "Init RegisterCenter Error.";
        return false;
    }
    else
    {
        LOG(INFO) << "Init RegisterCenter Succ.";
    }

    return true;
}

bool Application::StartProcess(
    const std::string &Port)
{
    std::string _addr = "0.0.0.0:" + Port;
    LOG(INFO) << "Application::StartProcess() addr:" << _addr;

    // 定义服务接收器
    grpcService = std::make_shared<GrpcReceiver>();

    grpc::ServerBuilder builder;
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_BDP_PROBE, 1);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
    builder.AddListeningPort(_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(grpcService.get()); // 注册服务接收器
    grpcServer = builder.BuildAndStart();
    if (grpcServer == nullptr)
    {
        LOG(ERROR) << "StartProcess Error";
        return false;
    }

    return true;
}

bool Application::Waiting()
{
    // 起线程 等待grpc线程退出
    std::thread serving_thread(
        [&]()
        {
            LOG(INFO) << "grpcServer Waiting ...";
            grpcServer->Wait();
        });

    // 等待退出消息
    auto quit = exit_requested.get_future();
    quit.wait();

    // 得到系统退出信号, 开始执行退出程序
    LOG(INFO) << "Get Shutdown Signal, exit StartProcess";

    // sleep 1s, 等待上级服务状态同步
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 停止grpc任务, 3秒等待grpc退出
    grpcServer->Shutdown(
        std::chrono::system_clock::now() +
        std::chrono::seconds(3));
    if (serving_thread.joinable())
    {
        serving_thread.join();
    }

    return true;
}

bool Application::Shutdown()
{
    grpcService = nullptr;                      // 清理服务接收器
    timer->Stop();                              // 停止定时器任务
    TDRedisConnPool::GetInstance()->ShutDown(); // 停止连接池任务
    return true;
}

void Application::notifyShutdown(int sigNum, siginfo_t *sigInfo, void *context)
{
    static std::once_flag oc; // 用于call_once的局部静态变量

    LOG(INFO) << "Application::notifyShutdown() sigNum = " << sigNum;
    std::call_once(oc, [&]()
                   { Application::GetInstance()->exit_requested.set_value(); });
}