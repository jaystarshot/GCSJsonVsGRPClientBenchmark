#include "google/cloud/storage/client.h"
#include "google/cloud/storage/grpc_plugin.h"
#include <chrono>
#include <iostream>
#include <vector>
#include <random>
#include <fstream>
#include <sstream>

namespace gcs = google::cloud::storage;
namespace gc = ::google::cloud;
using BenchmarkClock = std::chrono::high_resolution_clock;


struct BenchmarkResult {
    int64_t duration_ms;
    size_t bytes_read;
};

BenchmarkResult SequentialReadBenchmark(gcs::Client& client,
                                        const std::string& bucket,
                                        const std::string& object_name) {

    auto start_time = std::chrono::high_resolution_clock::now();
    auto stream = client.ReadObject(bucket, object_name);
    if (!stream) {
        std::cerr << "Error reading object: " << stream.status() << "\n";
        return {-1, 0};
    }

    constexpr std::size_t buffer_size = 4 * 1024 * 1024;
    std::vector<char> buffer(buffer_size);
    std::size_t total_bytes = 0;

    while (stream.read(buffer.data(), buffer.size())) {
        total_bytes += stream.gcount();
    }
    total_bytes += stream.gcount();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "[Run] Read " << total_bytes / (1024 * 1024)
              << " MB in " << duration_ms << " ms\n";

    return {duration_ms, total_bytes};
}

// Reads file randomly (multiple small random reads)
BenchmarkResult RandomReadBenchmark(gcs::Client& client,
                         const std::string& bucket,
                         const std::string& object_name,
                         std::size_t file_size,
                         std::size_t read_size = 4 * 1024 * 1024 /*4MB*/) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<std::size_t> dist(0, file_size - read_size);

    std::size_t total_bytes = 0;
    int num_reads = 0;

    auto start = BenchmarkClock::now();

    while (total_bytes < file_size) {
        std::size_t offset = dist(gen);
        auto stream = client.ReadObject(bucket, object_name,
                                        gcs::ReadRange(offset, offset + read_size - 1));
        if (!stream) {
            std::cerr << "Error in random read: " << stream.status() << "\n";
            break;
        }

        std::vector<char> buffer(read_size);
        stream.read(buffer.data(), buffer.size());
        total_bytes += stream.gcount();
        num_reads++;
    }

    auto end = BenchmarkClock::now();
    int64_t duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "[Random] Read approx. " << total_bytes / (1024 * 1024) << " MB in "
              << num_reads << " reads, over " << duration_ms << " ms\n";

    return BenchmarkResult{
        .duration_ms = duration_ms,
        .bytes_read = total_bytes
    };
}

void seq(int num_iterations, gcs::Client& client,
                         const std::string& bucket,
                         const std::string& object_name,
                         const std::string& tag) {
    int64_t total_duration = 0;
    int64_t min_duration = std::numeric_limits<int64_t>::max();
    int64_t max_duration = std::numeric_limits<int64_t>::min();
    size_t total_bytes = 0;
    auto metadata = client.GetObjectMetadata(bucket, object_name);
    if (!metadata) {
        std::cerr << "Error getting object metadata: " << metadata.status() << "\n";
        return;
    }
    std::cout <<tag + " \n==== Sequentially reading " << bucket << " " << object_name << " " << metadata->size() << " bytes\n";
    for (int i = 1; i <= num_iterations; ++i) {
        std::cout << "\n==== Iteration " << i << " ====\n";
        auto result = SequentialReadBenchmark(client, bucket, object_name);

        if (result.duration_ms < 0) {
            std::cerr << "Benchmark iteration failed, skipping stats.\n";
            continue;
        }

        total_duration += result.duration_ms;
        total_bytes = result.bytes_read; // same file each iteration
        if (result.duration_ms < min_duration) min_duration = result.duration_ms;
        if (result.duration_ms > max_duration) max_duration = result.duration_ms;
    }

    if (total_duration == 0) {
        std::cerr << "All iterations failed. No statistics available.\n";
        return;
    }

    double avg_duration = total_duration / static_cast<double>(num_iterations);

    std::cout << "\n==== Sequential Read Aggregate Benchmark Results ====\n";
    std::cout << "File size: " << total_bytes / (1024 * 1024) << " MB\n";
    std::cout << "Total iterations: " << num_iterations << "\n";
    std::cout << "Average read time: " << avg_duration << " ms\n";
    std::cout << "Min read time: " << min_duration << " ms\n";
    std::cout << "Max read time: " << max_duration << " ms\n";
}


void rand(int num_iterations, gcs::Client& client,
                         const std::string& bucket,
                         const std::string& object_name,
                          const std::string& tag) {
    int64_t total_duration = 0;
    int64_t min_duration = std::numeric_limits<int64_t>::max();
    int64_t max_duration = std::numeric_limits<int64_t>::min();
    size_t total_bytes = 0;
    auto metadata = client.GetObjectMetadata(bucket, object_name);
    if (!metadata) {
        std::cerr << "Error getting object metadata: " << metadata.status() << "\n";
        return;
    }
    std::cout <<tag + "\n==== Randomly reading " << bucket << " " << object_name << " " << metadata->size() << " bytes\n";
    for (int i = 1; i <= num_iterations; ++i) {
        std::cout << "\n==== Iteration " << i << " ====\n";
        auto result = RandomReadBenchmark(client, bucket, object_name, metadata->size());

        if (result.duration_ms < 0) {
            std::cerr << "Benchmark iteration failed, skipping stats.\n";
            continue;
        }

        total_duration += result.duration_ms;
        total_bytes = result.bytes_read; // same file each iteration
        if (result.duration_ms < min_duration) min_duration = result.duration_ms;
        if (result.duration_ms > max_duration) max_duration = result.duration_ms;
    }

    if (total_duration == 0) {
        std::cerr << "All iterations failed. No statistics available.\n";
        return;
    }

    double avg_duration = total_duration / static_cast<double>(num_iterations);

    std::cout << "\n==== Random Read Aggregate Benchmark Results ====\n";
    std::cout << "File size: " << total_bytes / (1024 * 1024) << " MB\n";
    std::cout << "Total iterations: " << num_iterations << "\n";
    std::cout << "Average read time: " << avg_duration << " ms\n";
    std::cout << "Min read time: " << min_duration << " ms\n";
    std::cout << "Max read time: " << max_duration << " ms\n";
}


int main(int argc, char* argv[]) {
    std::string bucket;
    std::string object_name;
    int numTimes = 5;
    auto options = gc::Options{};
    std::cout << "Args" << argc << "\n";
    if (argc == 5) {
        // std::cerr << "Usage: " << argv[0] << " <bucket-name> <object-name> <num-times> <json-cred>\n";
        // return 1;
        bucket = argv[1];
        object_name = argv[2];
        numTimes = std::stoi(argv[3]);


        auto credFile = argv[4];
        std::cout << "Using Json Creds\n" << credFile << "\n";
        std::ifstream jsonFile(credFile, std::ios::in);
        if (!jsonFile.is_open()) {
            std::cerr << "Error opening credentials file: " << credFile << "\n";
            return 1;
        }
        std::stringstream credsBuffer;
        credsBuffer << jsonFile.rdbuf();
        auto creds = credsBuffer.str();
        auto credentials = gc::MakeServiceAccountCredentials(std::move(creds));
        options.set<gc::UnifiedCredentialsOption>(credentials);
        // For test
    }else {
        bucket = "test1-gcs";
        object_name = "test-object-name";
        options.set<gcs::RestEndpointOption>("http://localhost:9001");
    }
    auto jsonClient = gcs::Client(options);
    auto grpcClient = gcs::MakeGrpcClient(options);
    seq(numTimes, grpcClient, bucket, object_name, "GRPC Client");
    seq(numTimes, jsonClient, bucket, object_name, "Json Client");
    rand(numTimes, grpcClient, bucket, object_name, "GRPC Client");
    rand(numTimes, jsonClient, bucket, object_name, "Json Client");

    return 0;
}