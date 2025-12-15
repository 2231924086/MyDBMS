cmake_minimum_required(VERSION 3.16)
project(DBMS LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
  add_compile_options(/W4 /permissive-)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

file(GLOB_RECURSE DBMS_CORE_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)

add_library(dbms_core ${DBMS_CORE_SOURCES})
target_include_directories(dbms_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")

add_executable(dbms "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_link_libraries(dbms PRIVATE dbms_core)

add_executable(dbms_tests "${CMAKE_CURRENT_SOURCE_DIR}/tests/dbms_tests.cpp")
target_link_libraries(dbms_tests PRIVATE dbms_core)

enable_testing()
add_test(NAME dbms_tests COMMAND dbms_tests)
