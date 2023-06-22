PROJECT(RegisterCenter)
CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

# grpc CMake Folder
SET(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "CommonLib/third_party/grpc/lib/cmake")
SET(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "CommonLib/third_party/grpc/lib64/cmake")

#build type
option(DEBUG "option for debug" OFF)
if (${DEBUG} STREQUAL "ON")
  message("debug build")
  add_definitions(-DDEBUG)
  remove_definitions(-DNDEBUG)
  remove_definitions(-DRELEASE)
  SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O2 -g -Wall -ggdb")
else()
  message("release build")
  add_definitions(-DRELEASE)
  add_definitions(-DNDEBUG)
  remove_definitions(-DDEBUG)
  SET(CMAKE_BUILD_TYPE "Release")
  SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O2 -g -Wall")
endif()

# 使用C++17版本编译
macro(require_cxx17)
  if (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} GREATER 3.0)
    # CMake 3.1 has built-in CXX standard checks.
    message("-- Setting C++17")
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED on)
  else()
    if (CMAKE_CXX_COMPILER_ID MATCHES "GCC")
      message ("-- GNU CXX (-std=c++17)")
      list(APPEND CMAKE_CXX_FLAGS "-std=c++17")
    elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
      message ("-- CLang CXX (-std=c++17)")
      list(APPEND CMAKE_CXX_FLAGS "-std=c++17")
    else()
      message (" requires C++17. Your compiler does not support it.")
    endif()
  endif()
endmacro()

# 查找protobuf
macro( require_protobuf )
  find_package(Protobuf CONFIG REQUIRED)
  if(Protobuf_FOUND)
    message(STATUS "Using protobuf ${Protobuf_VERSION}")
  else()
    message(FATAL_ERROR "protobuf library is needed but can't be found.")
  endif()
  set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
  set(_REFLECTION gRPC::grpc++_reflection)
  find_program(_PROTOBUF_PROTOC protoc)
endmacro()

# 查找grpc
# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
macro( require_grpc )
  find_package(gRPC CONFIG REQUIRED)
  message(STATUS "Using gRPC ${gRPC_VERSION}")
  set(_GRPC_GRPCPP gRPC::grpc++)
  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
endmacro()

# 查找多线程库
macro( require_thread )
  find_package(Threads REQUIRED)
endmacro()

require_cxx17()
require_protobuf()
require_grpc()
require_thread()

# include directories
INCLUDE_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}/Src
  ${PROJECT_SOURCE_DIR}/CommonLib
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/TDRedis
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/grpc/include
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/curl/include
  ${PROJECT_SOURCE_DIR}/CommonLib/Protobuf/
)

# lib directories
LINK_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/glog
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/TDRedis
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/TCMalloc
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/grpc/lib
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/grpc/lib64
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/curl/lib
  ${PROJECT_SOURCE_DIR}/CommonLib/third_party/nghttp2
)

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/../../Bin)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/Bin/Lib)
SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/Bin/Lib)

# use TCMalloc
link_libraries(tcmalloc_minimal.a)

ADD_SUBDIRECTORY( CommonLib/Protobuf )
ADD_SUBDIRECTORY( CommonLib/signal )
ADD_SUBDIRECTORY( CommonLib/timer )
ADD_SUBDIRECTORY( CommonLib/GrpcDispatcher )
ADD_SUBDIRECTORY( CommonLib/MurmurHash3)
ADD_SUBDIRECTORY( CommonLib/LarkMessage )

ADD_SUBDIRECTORY( Src/RegisterCenter )
ADD_SUBDIRECTORY( Src/RedisInteracts )
