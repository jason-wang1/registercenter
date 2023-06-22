#pragma once
#include <string>
#include <atomic>
#include <any>
#include <shared_mutex>
#include "ClientManager/GrpcSender.h"

// 服务接口类
class RegisterCenter
{
public:
    bool Init();

    static bool OnHello(
        std::any obj,
        const int cmd,
        const long deadline_ms,
        const std::string &request,
        int &result,
        std::string &response);

    static bool OnRegister(
        std::any obj,
        const int cmd,
        const long deadline_ms,
        const std::string &request,
        int &result,
        std::string &response);

    static bool OnOnline(
        std::any obj,
        const int cmd,
        const long deadline_ms,
        const std::string &request,
        int &result,
        std::string &response);

    static bool OnOffline(
        std::any obj,
        const int cmd,
        const long deadline_ms,
        const std::string &request,
        int &result,
        std::string &response);

    static bool OnPing(
        std::any obj,
        const int cmd,
        const long deadline_ms,
        const std::string &request,
        int &result,
        std::string &response);

    static bool OnCheck(
        std::any obj,
        const int cmd,
        const long deadline_ms,
        const std::string &request,
        int &result,
        std::string &response);
};
