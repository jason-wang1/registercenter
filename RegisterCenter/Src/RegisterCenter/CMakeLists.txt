aux_source_directory(. ALL_FILES)
ADD_EXECUTABLE(RegisterCenter ${ALL_FILES})

TARGET_LINK_LIBRARIES( 
    RegisterCenter
    Protobuf
    glog
    CSignal
    CTimer
    TDRedis
    LarkMessage
    RedisInteracts
    ${_REFECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)
