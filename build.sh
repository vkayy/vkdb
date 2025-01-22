#!/bin/bash

RUN_TESTS=false
RUN_BENCHMARKS=false
RUN_EXAMPLE=false
EXAMPLE_FILE=""
BUILD_DIR="build"

print_usage() {
    echo "Usage: $0 [--run-tests|-t] [--run-benchmarks|-b] [--run-example|-e <filename>]"
    exit 1
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --run-tests|-t)
            RUN_TESTS=true
            ;;
        --run-benchmarks|-b)
            RUN_BENCHMARKS=true
            ;;
        --run-example|-e)
            RUN_EXAMPLE=true
            if [[ -n $2 && $2 != -* ]]; then
                EXAMPLE_FILE=$2
                shift
            else
                print_usage
            fi
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            ;;
    esac
    shift
done

if [ ! -d "$BUILD_DIR" ]; then
    echo "Creating build directory..."
    mkdir "$BUILD_DIR" || { echo "Failed to create directory '$BUILD_DIR'"; exit 1; }
fi

cd "$BUILD_DIR" || { echo "Failed to change directory to '$BUILD_DIR'"; exit 1; }

if ! cmake -DCMAKE_BUILD_TYPE=Release ..; then
    echo "CMake configuration failed."
    exit 1
fi

if ! make; then
    echo "Make failed."
    exit 1
fi

clear

echo "Built successfully."

if [ "$RUN_TESTS" = true ]; then
    echo "Running tests..."
    if [ -x ./test/vkdb_tests ]; then
        ./test/vkdb_tests
    else
        echo "Test executable not found or not executable."
    fi
fi

if [ "$RUN_BENCHMARKS" = true ]; then
    echo "Running benchmarks..."
    if [ -x ./benchmark/vkdb_benchmarks ]; then
        ./benchmark/vkdb_benchmarks
    else
        echo "Benchmark executable not found or not executable."
    fi
fi

if [ "$RUN_EXAMPLE" = true ]; then
    echo "Running example '$EXAMPLE_FILE'..."
    if [ -x "./examples/$EXAMPLE_FILE" ]; then
        "./examples/$EXAMPLE_FILE"
    else
        echo "Example file './examples/$EXAMPLE_FILE' not found or not executable."
    fi
fi

cd ..
