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
)

set_target_properties(search PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    INTERPROCEDURAL_OPTIMIZATION ON
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
)

target_include_directories(search PRIVATE "include")
