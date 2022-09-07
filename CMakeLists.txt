cmake_minimum_required(VERSION 3.6)

project(Rep VERSION 1.0)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

message(STATUS "CMAKE_SYSTEM: ${CMAKE_SYSTEM}")
message(STATUS "CMAKE_VERSION: ${CMAKE_VERSION}")

message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_INCLUDE_PATH: ${CMAKE_INCLUDE_PATH}")
message(STATUS "CMAKE_LIBRARY_PATH: ${CMAKE_LIBRARY_PATH}")
message(STATUS "PROJECT_NAME: ${PROJECT_NAME}")

add_subdirectory(proto)

add_executable(client cmain.cc 
      ./client/client.cc
      ./common/common.cc)

add_executable(server smain.cc
      ./common/txn.cc
      ./common/convert.cc
      ./common/common.cc)

target_include_directories(client PRIVATE
      ${PROJECT_SOURCE_DIR}/proto/include 
      ${PROJECT_SOURCE_DIR}/client
      ${PROJECT_SOURCE_DIR}/common)

target_include_directories(server PRIVATE
      ${PROJECT_SOURCE_DIR}/proto/include
      ${PROJECT_SOURCE_DIR}/common)

target_link_libraries(client PRIVATE Proto zmqpp zmq)

target_link_libraries(server PRIVATE Proto pthread zmqpp zmq)

# 编译器警告信息flag
set(gcc_like_cxx "$<COMPILE_LANG_AND_ID:CXX,ARMClang,AppleClang,Clang,GNU,LCC>")
set(msvc_cxx "$<COMPILE_LANG_AND_ID:CXX,MSVC>")
target_compile_options(server INTERFACE
  "$<${gcc_like_cxx}:$<BUILD_INTERFACE:-Wall;-Wextra;-Wshadow;-Wformat=2;-Wunused>>"
  "$<${msvc_cxx}:$<BUILD_INTERFACE:-W3>>"
)
target_compile_options(client INTERFACE
  "$<${gcc_like_cxx}:$<BUILD_INTERFACE:-Wall;-Wextra;-Wshadow;-Wformat=2;-Wunused>>"
  "$<${msvc_cxx}:$<BUILD_INTERFACE:-W3>>"
)