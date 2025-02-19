#!/bin/bash

RUN_TESTS=false
RUN_BENCHMARKS=false
RUN_EXAMPLE=false
EXAMPLE_FILE=""
BUILD_DIR="build"
PROFILE_PATH="$HOME/.conan2/profiles/default"
MIN_VERSION=20

echo_error() {
    echo -e "\033[31mError: $1\033[0m";
}

echo_success() {
    echo -e "\033[32m$1\033[0m";
}

echo_header() {
    echo -e "\n\033[34m=== $1 ===\033[0m";
}

echo_info() {
    echo -e "\033[33m$1\033[0m";
}

print_usage() {
    echo_info "Usage: $0 [--run-tests|-t] [--run-benchmarks|-b] [--run-example|-e <filename>]"
    exit 1
}

change_directory() {
    if ! cd "$1" ; then
        echo_error "Failed to change directory to '$1'"
        exit 1
    fi
}

require_command() {
    echo_header "Checking for $1..."
    if ! command -v "$1" &> /dev/null; then
        echo_error "$1 could not be found."
        exit 1
    fi
}

parse_arguments() {
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
                echo_error "Unknown option '$1'."
                print_usage
                ;;
        esac
        shift
    done
}

validate_conan_profile() {
    echo_header "Validating Conan profile..."

    if [ ! -f "$PROFILE_PATH" ]; then
        echo_error "Conan profile not found at $PROFILE_PATH."
        exit 1
    fi

    local cppstd_line=$(grep -E "^compiler\.cppstd=" "$PROFILE_PATH" || true)
    if [ -z "$cppstd_line" ]; then
        echo_error "C++ standard not set in Conan profile."
        exit 1
    fi

    local cppstd_value=$(echo "$cppstd_line" | cut -d'=' -f2 | tr -d '[:space:]')
    local version=$(echo "$cppstd_value" | grep -oE '[0-9]+' | head -n1)

    if [ -z "$version" ]; then
        echo_error "Failed to parse C++ standard version from Conan profile."
        exit 1
    fi

    if [ "$version" -lt "$MIN_VERSION" ]; then
        echo_error "Minimum required C++ standard version is $MIN_VERSION, found $version."
        exit 1
    fi
}

install_dependencies() {
    echo_header "Installing dependencies with Conan..."

    if ! conan install . --output-folder="$BUILD_DIR" --build=missing; then
        echo_error "Dependency installation with Conan failed."
        exit 1
    fi
}

configure_project() {
    echo_header "Configuring project with CMake..."

    if ! cmake --preset conan-release .. ; then
        echo_error "Project configuration with CMake failed."
        exit 1
    fi
}

compile_project() {
    echo_header "Compiling project with CMake..."

    if ! cmake --build . ; then
        echo_error "Compilation with CMake failed."
        exit 1
    fi
}

type_to_name() {
    local type=$1
    case $type in
        test) echo "tests" ;;
        benchmark) echo "benchmarks" ;;
        examples) echo "examples" ;;
    esac
}

run_component() {
    local type=$1
    local name=$(type_to_name $type)
    local executable="$1/$2"

    echo_header "Running $name..."

    [[ -x "$executable" ]] && {
        "$executable"
    } || {
        echo_error "File '$2' not found in $name."
        return 1
    }
}

main() {
    parse_arguments "$@"

    require_command conan
    validate_conan_profile

    install_dependencies

    change_directory "$BUILD_DIR"

    configure_project
    compile_project

    clear

    echo_header "Built successfully."

    "$RUN_TESTS" && run_component "test" "vkdb_tests"
    "$RUN_BENCHMARKS" && run_component "benchmark" "vkdb_benchmarks"
    "$RUN_EXAMPLE" && run_component "examples" "$EXAMPLE_FILE"

    cd ..
}

main "$@"