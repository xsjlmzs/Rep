
# FindProtobuf.cmake
set(PROTOBUF_DIR "/usr/local/protobuf/include" "/usr/local/protobuf/lib")

list(APPEND CMAKE_PREFIX_PATH ${PROTOBUF_DIR})
find_package(Protobuf REQUIRED)


file(GLOB pb_files ${CMAKE_CURRENT_SOURCE_DIR}/proto/*.proto)
set(DST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(PROTO_FLAGS "-I${CMAKE_CURRENT_SOURCE_DIR}/proto")

foreach(FIL ${pb_files})
    get_filename_component(FIL_NAME ${FIL} NAME_WE)

    list(APPEND PROTO_SRCS ${DST_DIR}/${FIL_NAME}.pb.cc)
    list(APPEND PROTO_HDRS ${DST_DIR}/${FIL_NAME}.pb.h)

    # execute_process(COMMAND ${Protobuf_PROTOC_EXECUTABLE} ${PROTO_FLAGS} --cpp_out=${DST_DIR} ${FIL})

    add_custom_command(
        OUTPUT
            ${DST_DIR}/${FIL_NAME}.pb.cc
            ${DST_DIR}/${FIL_NAME}.pb.h
        COMMAND
            ${Protobuf_PROTOC_EXECUTABLE} ${PROTO_FLAGS} --cpp_out=${DST_DIR} ${FIL}
        DEPENDS
            ${FIL}
        COMMENT
            "generate pb.cc & pb.h"
        VERBATIM
    )

endforeach()

add_custom_target(pb_cpp ALL
    DEPENDS
        ${PROTO_SRCS}
        ${PROTO_HDRS}
    COMMENT
        "generate target pb_cpp"
    VERBATIM
)

add_library(Proto ${PROTO_SRCS})

# 通过pkg-config获得protobuf的编译参数
find_package(PkgConfig REQUIRED)
pkg_check_modules(PB REQUIRED IMPORTED_TARGET protobuf)
target_link_libraries(Proto PkgConfig::PB)

# target_include_directories(Proto PUBLIC ${Protobuf_INCLUDE_DIRS})
# target_link_directories(Proto PRIVATE ${Protobuf_LIBRARIES})

# 安装
install(TARGETS Proto DESTINATION lib)
install(FILES ${PROTO_HDRS} DESTINATION include)