aux_source_directory(. LIB_SRCS)
add_library(RedisInteracts STATIC ${LIB_SRCS})

TARGET_LINK_LIBRARIES(
    RedisInteracts
    TDRedis
    glog
)
