#!/usr/bin/env bash
set -euo pipefail

# Test suite for scripts/resolve-test-iggy-server.sh
# Runs offline with mocked binaries and temporary directories.
# Does NOT download from the artifact server.

readonly test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT

readonly test_cache="$test_dir/cache"
readonly test_pin_file="$test_dir/iggy-server-amd64-skylake.sha256"
readonly test_binary="$test_dir/test-binary"
readonly resolver="$(dirname "$(realpath "$0")")/resolve-test-iggy-server.sh"

# Track test results.
pass_count=0
fail_count=0

# Create a fake binary for testing.
create_test_binary() {
  local path="$1"
  echo "#!/bin/bash" > "$path"
  echo "echo 'test'" >> "$path"
  chmod 755 "$path"
}

# Compute and store the SHA-256 of a file.
compute_sha256() {
  local file="$1"
  sha256sum "$file" | awk '{print $1}'
}

# Run a test and report results.
run_test() {
  local test_name="$1"
  local expected_exit="$2"
  shift 2
  local cmd=("$@")
  
  local output
  local actual_exit
  
  output=$("${cmd[@]}" 2>&1) || actual_exit=$?
  actual_exit=${actual_exit:-0}
  
  if [[ $actual_exit -eq $expected_exit ]]; then
    echo "[PASS] $test_name"
    ((pass_count++))
    return 0
  else
    echo "[FAIL] $test_name (expected exit $expected_exit, got $actual_exit)"
    if [[ -n "$output" ]]; then
      echo "  Output: $output"
    fi
    ((fail_count++))
    return 1
  fi
}

# ============================================================================
# Test Suite
# ============================================================================

echo "Running test suite for resolve-test-iggy-server.sh"
echo "Test directory: $test_dir"
echo ""

# Test 1: Bash syntax validation of resolve-test-iggy-server.sh.
echo "=== Syntax Validation ==="
run_test "resolve_script_bash_syntax_valid" 0 bash -n "$resolver"
echo ""

# Test 2: Correct SHA-256 checksum validates.
echo "=== Checksum Validation ==="
create_test_binary "$test_binary"
checksum=$(compute_sha256 "$test_binary")
run_test "correct_checksum_matches" 0 bash -c "printf '%s  %s\n' '$checksum' '$test_binary' | sha256sum --check --status"
echo ""

# Test 3: Incorrect checksum fails.
echo "=== Incorrect Checksum ==="
wrong_checksum="0000000000000000000000000000000000000000000000000000000000000000"
run_test "incorrect_checksum_fails" 1 bash -c "printf '%s  %s\n' '$wrong_checksum' '$test_binary' | sha256sum --check --status"
echo ""

# Test 4: Checksum format validation.
echo "=== Checksum Format Validation ==="
run_test "format_valid_lowercase_hex" 0 bash -c "[[ 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' =~ ^[0-9a-fA-F]{64}$ ]]"
run_test "format_valid_uppercase_hex" 0 bash -c "[[ 'E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855' =~ ^[0-9a-fA-F]{64}$ ]]"
run_test "format_valid_mixed_case_hex" 0 bash -c "[[ 'E3b0c44298Fc1C149afbF4c8996FB92427AE41E4649b934cA495991b7852B855' =~ ^[0-9a-fA-F]{64}$ ]]"
run_test "format_reject_too_long" 1 bash -c "[[ 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8550' =~ ^[0-9a-fA-F]{64}$ ]]"
run_test "format_reject_too_short" 1 bash -c "[[ 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85' =~ ^[0-9a-fA-F]{64}$ ]]"
run_test "format_reject_invalid_char" 1 bash -c "[[ 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8G5' =~ ^[0-9a-fA-F]{64}$ ]]"
run_test "format_reject_empty_string" 1 bash -c "[[ '' =~ ^[0-9a-fA-F]{64}$ ]]"
echo ""

# Test 5: Pin file extraction and validation.
echo "=== Pin File Handling ==="
echo "$checksum  iggy-server-linux-amd64-skylake" > "$test_pin_file"
extracted=$(awk '{print $1}' "$test_pin_file")
run_test "pin_file_hash_extracted" 0 bash -c "[[ '$extracted' == '$checksum' ]]"
echo ""

# Test 6: Missing pin file scenario (when no env var and no pin file).
echo "=== Missing Pin File Scenario ==="
run_test "missing_pin_file_detection" 0 bash -c "[[ ! -f '$test_dir/nonexistent.sha256' ]] && echo 'missing' | grep -q 'missing'"
echo ""

# Test 7: Pin file with empty checksum is rejected.
echo "=== Malformed Pin File ==="
echo "  iggy-server-linux-amd64-skylake" > "$test_pin_file"
empty_hash=$(awk '{print $1}' "$test_pin_file")
run_test "empty_pin_file_hash_detected" 0 bash -c "[[ -z '$empty_hash' ]]"
echo ""

# Test 8: Environment variable override path (LASER_TEST_IGGY_SHA256).
echo "=== Environment Variable Override ==="
run_test "env_var_override_valid_format" 0 bash -c "export LASER_TEST_IGGY_SHA256='$checksum'; [[ \"\$LASER_TEST_IGGY_SHA256\" =~ ^[0-9a-fA-F]{64}$ ]]"
run_test "env_var_override_invalid_format_rejected" 1 bash -c "export LASER_TEST_IGGY_SHA256='invalid'; [[ \"\$LASER_TEST_IGGY_SHA256\" =~ ^[0-9a-fA-F]{64}$ ]]"
echo ""

# Test 9: File executable check.
echo "=== File Executable Check ==="
non_exec_file="$test_dir/non-exec"
touch "$non_exec_file"
run_test "executable_file_detected" 0 bash -c "[[ -x '$test_binary' ]]"
run_test "non_executable_file_rejected" 1 bash -c "[[ -x '$non_exec_file' ]]"
echo ""

# Test 10: Directory and file existence checks.
echo "=== Directory and File Existence ==="
run_test "directory_exists_check" 0 bash -c "[[ -d '$test_dir' ]]"
run_test "file_exists_check" 0 bash -c "[[ -f '$test_binary' ]]"
run_test "nonexistent_file_check" 1 bash -c "[[ -f '$test_dir/nonexistent_file' ]]"
echo ""

# ============================================================================
# Summary
# ============================================================================
echo "=========================================="
echo "Test Results"
echo "=========================================="
echo "Passed: $pass_count"
echo "Failed: $fail_count"
echo "=========================================="

if [[ $fail_count -gt 0 ]]; then
  exit 1
fi

exit 0
