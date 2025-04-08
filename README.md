# Benchmark: GCS C++ JSON vs gRPC Client

This benchmark compares performance between the Google Cloud Storage (GCS) C++ client using the JSON REST API and the gRPC client.

Note: we must set the GOOGLE_APPLICATION_CREDENTIALS env variable to the <path-to-credentials.json> before testing. This makes sure that direct-path connectivity is tested too


## Prerequisites

Ensure the following libraries are available on your system:

https://github.com/googleapis/google-cloud-cpp 

- `storage_client`
- `storage_client_grpc`

## Build Instructions

1. Compile the project with make
   ```
   make
   ```
## Run Benchmark
cd build/

```
./benchmark <bucket-name> <object-name> <no-of-iterations> <path-to-credentials.json>
```



