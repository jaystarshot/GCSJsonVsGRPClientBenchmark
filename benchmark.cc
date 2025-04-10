#include "google/cloud/storage/client.h"
#include "google/cloud/storage/grpc_plugin.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <stdexcept>
#include <vector>
#include <string>

namespace gcs = google::cloud::storage;
namespace gc = ::google::cloud;

using BenchmarkClock = std::chrono::high_resolution_clock;

constexpr std::size_t kKiB = 1024;
constexpr std::size_t kMiB = 1024 * kKiB;
constexpr std::size_t kDefaultBufferSize = 4 * kMiB;
constexpr int kErrorDuration = -1;

struct BenchmarkResult {
    int64_t duration_ms = kErrorDuration;
    size_t bytes_read = 0;
};

BenchmarkResult SequentialReadBenchmark(gcs::Client &client,
                                        const std::string &bucket,
                                        const std::string &object_name,
                                        size_t buffer_size = kDefaultBufferSize) {
    BenchmarkResult result;
    auto start_time = BenchmarkClock::now();

    auto stream = client.ReadObject(bucket, object_name);
    if (!stream) {
        std::cerr << "Error opening object for sequential read: " << stream.status() << "\n";
        return result;
    }

    std::vector<char> buffer(buffer_size);
    std::size_t total_bytes = 0;

    while (stream.read(buffer.data(), buffer.size())) {
        total_bytes += stream.gcount();
    }

    if (!stream.eof()) {
         std::cerr << "Error during sequential read: " << stream.status() << "\n";
         return result;
    }
    total_bytes += stream.gcount();

    auto end_time = BenchmarkClock::now();
    result.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    result.bytes_read = total_bytes;

    return result;
}

// Reads file at random offsets, with non-overlapping ranges
BenchmarkResult RandomReadBenchmark(gcs::Client &client,
                                    const std::string &bucket,
                                    const std::string &object_name,
                                    std::size_t file_size,
                                    std::size_t read_size = kDefaultBufferSize) {
    BenchmarkResult result;
    if (file_size == 0) {
        std::cerr << "Error: file_size cannot be 0 for random reads.\n";
        result.duration_ms = 0;
        return result;
    }
    if (read_size == 0) {
        std::cerr << "Error: read_size cannot be 0 for random reads.\n";
        return result;
    }

    std::vector<std::size_t> offsets;
    offsets.reserve(file_size / read_size + 1);
    for (size_t current_offset = 0; current_offset < file_size; current_offset += read_size) {
        offsets.push_back(current_offset);
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(offsets.begin(), offsets.end(), gen);

    std::size_t total_bytes_read = 0;
    auto start_time = BenchmarkClock::now();
    std::vector<char> buffer(read_size);

    for (const auto &offset : offsets) {
        std::size_t bytes_to_read = std::min(read_size, file_size - offset);
        if (bytes_to_read == 0) continue;

        auto stream = client.ReadObject(bucket, object_name, gcs::ReadRange(offset, offset + bytes_to_read));
        if (!stream) {
            std::cerr << "Error opening object for random read at offset " << offset << ": " << stream.status() << "\n";
            result.bytes_read = total_bytes_read;
            return result;
        }

        stream.read(buffer.data(), bytes_to_read);
        std::streamsize chunk_bytes_read = stream.gcount();

        if (!stream.eof() && stream.fail()) {
             std::cerr << "Error during random read at offset " << offset << ": " << stream.status() << "\n";
             result.bytes_read = total_bytes_read + chunk_bytes_read;
             return result;
        }

        total_bytes_read += chunk_bytes_read;
    }

    auto end_time = BenchmarkClock::now();
    result.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    result.bytes_read = total_bytes_read;

    return result;
}

std::string GetTimestamp() {
    auto system_now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(system_now);
    char time_buf[80];
    if (std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now_c)) == 0) {
         return "Timestamp Error";
    }
    return std::string(time_buf);
}


void PrintAggregateResults(const std::string &type,
                           int num_iterations_attempted,
                           size_t file_size_bytes,
                           size_t read_size_bytes,
                           const std::vector<int64_t> &successful_durations)
{
    int successful_iterations = successful_durations.size();

    std::cout << "\n==== " << type << " Read Aggregate Benchmark Results ====\n";
    double file_size_mb = file_size_bytes / static_cast<double>(kMiB);
    std::cout << "File size: " << file_size_mb << " MB (" << file_size_bytes << " bytes)\n";
    if (read_size_bytes > 0) {
        std::cout << "Read size: " << read_size_bytes / kKiB << " KB" << "\n";
    }
    std::cout << "Total successful iterations: " << successful_iterations << " / " << num_iterations_attempted << "\n";

    if (successful_iterations == 0) {
        std::cout << "No successful iterations. No statistics available.\n";
        return;
    }

    std::vector<int64_t> sorted_durations = successful_durations;
    std::sort(sorted_durations.begin(), sorted_durations.end());

    int64_t total_duration = std::accumulate(sorted_durations.begin(), sorted_durations.end(), 0LL);
    double avg_duration = static_cast<double>(total_duration) / successful_iterations;

    size_t p50_index = static_cast<size_t>(std::floor(0.5 * (successful_iterations - 1)));
    int64_t p50_duration = sorted_durations[p50_index];

    size_t p90_index = static_cast<size_t>(std::floor(0.9 * (successful_iterations - 1)));
    p90_index = std::min(p90_index, sorted_durations.size() - 1);
    int64_t p90_duration = sorted_durations[p90_index];

    int64_t min_duration = sorted_durations.front();
    int64_t max_duration = sorted_durations.back();

    double avg_throughput_mbs = (avg_duration > 0)
                                    ? (file_size_mb / (avg_duration / 1000.0))
                                    : 0.0;

    std::cout << "Average (mean) time: " << avg_duration << " ms\n";
    std::cout << "P50 (median) time:    " << p50_duration << " ms\n";
    std::cout << "P90 time:             " << p90_duration << " ms\n";
    std::cout << "Min time:             " << min_duration << " ms\n";
    std::cout << "Max time:             " << max_duration << " ms\n";
    std::cout << "Average throughput:   " << avg_throughput_mbs << " MB/s\n";
}

void RunSequentialBenchmark(int num_iterations, gcs::Client &client,
                         const std::string &bucket,
                         const std::string &object_name,
                         const std::string &tag) {
    auto metadata = client.GetObjectMetadata(bucket, object_name);
    if (!metadata) {
         std::cerr << "Error getting metadata for " << bucket << "/" << object_name << ": " << metadata.status() << "\n";
         return;
    }
    size_t file_size_bytes = metadata->size();

    std::cout << "\n" << tag << "\n==== Sequentially reading " << bucket << "/" << object_name
              << " (" << file_size_bytes / static_cast<double>(kMiB) << " MB)"
              << " Buffer size: " << kDefaultBufferSize / kKiB << " KB ====\n";

    std::vector<int64_t> durations;
    durations.reserve(num_iterations);

    for (int i = 1; i <= num_iterations; ++i) {
        auto result = SequentialReadBenchmark(client, bucket, object_name, kDefaultBufferSize);
         std::cout << "[" << GetTimestamp() << "] Iteration " << i << ": ";
        if (result.duration_ms != kErrorDuration) {
            std::cout << result.bytes_read / kMiB << " MB in " << result.duration_ms << " ms\n";
            durations.push_back(result.duration_ms);
        } else {
            std::cout << "Failed.\n";
        }
    }

    PrintAggregateResults("Sequential (" + tag + ")", num_iterations, file_size_bytes, 0, durations);
}

void RunRandomBenchmark(int num_iterations, gcs::Client &client,
                     const std::string &bucket,
                     const std::string &object_name,
                     std::size_t read_size,
                     const std::string &tag) {
    auto metadata = client.GetObjectMetadata(bucket, object_name);
     if (!metadata) {
         std::cerr << "Error getting metadata for " << bucket << "/" << object_name << ": " << metadata.status() << "\n";
         return;
    }
    size_t file_size_bytes = metadata->size();

    std::cout << "\n" << tag << "\n==== Random reading " << bucket << "/" << object_name
              << " (" << file_size_bytes / static_cast<double>(kMiB) << " MB)"
              << " Read size: " << read_size / kKiB << " KB ====\n";

    std::vector<int64_t> durations;
    durations.reserve(num_iterations);

    for (int i = 1; i <= num_iterations; ++i) {
        auto result = RandomReadBenchmark(client, bucket, object_name, file_size_bytes, read_size);
        std::cout << "[" << GetTimestamp() << "] Iteration " << i << ": ";
        if (result.duration_ms != kErrorDuration) {
            std::cout << result.bytes_read / kMiB << " MB in " << result.duration_ms << " ms\n";
            durations.push_back(result.duration_ms);
        } else {
             std::cout << "Failed. Read " << result.bytes_read / static_cast<double>(kMiB) << " MB before failure.\n";
        }
    }

    PrintAggregateResults("Random (" + tag + ")", num_iterations, file_size_bytes, read_size, durations);
}


// The GCS service account key file should be passed via an env var
// export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_key.json
int main(int argc, char *argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: benchmark <bucket> <object> <times>\n";
        return 1;
    }

    std::string bucket = argv[1];
    std::string object_name = argv[2];
    int numTimes = 5;
    try {
        numTimes = std::stoi(argv[3]);
        if (numTimes <= 0) {
             std::cerr << "Error: Number of times must be positive.\n";
             return 1;
        }
    } catch (const std::invalid_argument &ia) {
        std::cerr << "Error: Invalid number for times: " << argv[3] << '\n';
        return 1;
    } catch (const std::out_of_range &oor) {
        std::cerr << "Error: Number of times out of range: " << argv[3] << '\n';
        return 1;
    }

    auto options = gc::Options{};
    auto jsonClient = gcs::Client(options);
    auto grpcClient = gcs::MakeGrpcClient(options);

    RunSequentialBenchmark(numTimes, grpcClient, bucket, object_name, "GRPC Client");
    RunSequentialBenchmark(numTimes, jsonClient, bucket, object_name, "Json Client");

    std::vector<std::size_t> read_sizes = {
        4 * kMiB,
        2 * kMiB,
        1 * kMiB,
        100 * kKiB
    };

    for (auto size : read_sizes) {
        RunRandomBenchmark(numTimes, grpcClient, bucket, object_name, size, "GRPC Client");
        RunRandomBenchmark(numTimes, jsonClient, bucket, object_name, size, "JSON Client");
    }

    return 0;
}