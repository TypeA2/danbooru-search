cmake_minimum_required(VERSION 3.25)

# Add vcpkg toolchain file dynamically
find_program(VCPKG_EXECUTABLE vcpkg)
if(VCPKG_EXECUTABLE)
    cmake_path(GET VCPKG_EXECUTABLE PARENT_PATH VCPKG_ROOT)
    set(CMAKE_TOOLCHAIN_FILE "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
endif()

project(danbooru-search VERSION 1.0.0)

add_executable(search
    "src/search.cpp"
    "include/helper.hpp"
)

set_target_properties(search PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    INTERPROCEDURAL_OPTIMIZATION ON
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
)

add_executable(parse
    "src/parse.cpp"
    "include/helper.hpp"
)

set_target_properties(parse PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    INTERPROCEDURAL_OPTIMIZATION ON
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
)

find_package(simdjson CONFIG REQUIRED)
target_link_libraries(parse PRIVATE simdjson::simdjson)

if (MSVC)
    target_compile_options(search PRIVATE /W3 /arch:avx2)
    target_compile_options(parse PRIVATE /W3 /arch:avx2)
else()
    target_compile_options(search PRIVATE -Wall -Wextra -pedantic -march=native)
    target_compile_options(parse PRIVATE -Wall -Wextra -pedantic -march=native)
endif()

target_include_directories(search PRIVATE "include")
target_include_directories(parse PRIVATE "include")
