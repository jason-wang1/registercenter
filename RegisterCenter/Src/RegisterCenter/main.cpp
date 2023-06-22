#include <string>
#include "glog/logging.h"
#include "glog/raw_logging.h"

#include "Define.h"
#include "Application.h"

#include "Common/Function.h"

void glog_init(const std::string &log_dir,
               const std::string &name)
{
    FLAGS_log_dir = log_dir;
    google::InitGoogleLogging(name.c_str());
    FLAGS_stop_logging_if_full_disk = true;
    FLAGS_logbufsecs = 0; // 设置可以缓冲日志的最大秒数，0指实时输出
}

void glog_Shutdown()
{
    google::ShutdownGoogleLogging();
}

int main(int argc, char **argv)
{
    if (argc < 4)
    {
        // 需要服务IP、Port、Group、Name
        return -1;
    }
    srand(time(NULL));

    std::string Path(argv[0]);
    std::string IP(argv[1]);
    std::string Port(argv[2]);
    std::string Nicename(argv[3]);
    std::string name(argv[4]);

    // 创建日志文件夹
    auto Exe = std::filesystem::path(Path).filename().string();
    const std::string log_dir = "../Log" + Exe;
    std::string failed_info;
    if (!Common::MkdirPath(log_dir, 0755, failed_info))
    {
        // 创建文件夹失败
        std::cout << "MkdirPath Failed, info = " << failed_info << std::endl;
        return 1;
    }

    glog_init(log_dir, name);

#ifdef DEBUG
    LOG(INFO) << "Debug Project Service Manager Version: " << Common::APP_VERSION;
#elif RELEASE
    LOG(INFO) << "Release Project Service Manager Version: " << Common::APP_VERSION;
#else
    LOG(ERROR) << "Not Known " << name << " Debug Or Release Version: " << Common::APP_VERSION;
#endif

    auto app = Application::GetInstance();
    if (app->Init(IP, Port, Exe, Nicename))
    {
        if (app->StartProcess(Port))
        {
            app->Waiting();
        }
    }

    app->Shutdown();

    LOG(INFO) << "Process Quit()";

    glog_Shutdown();
    return 0;
}
