cmake_minimum_required(VERSION 3.16)

project(GCSClientBenchmark CXX)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# Find the Google Cloud Storage packages
find_package(google_cloud_cpp_storage REQUIRED)
find_package(google_cloud_cpp_storage_grpc REQUIRED)

add_executable(benchmark benchmark.cc)

target_link_libraries(benchmark
        google-cloud-cpp::storage
        google-cloud-cpp::storage_grpc
)
