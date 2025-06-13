cmake_minimum_required(VERSION 3.15)
project(sort_merge_join LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB HDRS CONFIGURE_DEPENDS "include/*.hpp")
file(GLOB SRCS CONFIGURE_DEPENDS "src/*.cpp" "main.cpp")

add_executable(smj ${SRCS} ${HDRS})
target_include_directories(smj PRIVATE include)
