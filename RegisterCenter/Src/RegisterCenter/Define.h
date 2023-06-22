#pragma once
#include <string>
#include <ostream>

#include "Common/Error.h"

namespace Common
{
    // 注册中心版本号
    static const std::string APP_VERSION = "1.0.0";

    // APP配置路径
    const static std::string ConfigPath_App = "./config/RegisterCenterConf.json";

} // namespace Common
