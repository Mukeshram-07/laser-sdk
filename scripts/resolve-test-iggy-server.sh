#!/usr/bin/env bash
set -euo pipefail

# Resolve the Iggy server binary the test harnesses run. An explicit
# LASER_TEST_IGGY_SERVER binary wins, otherwise the pinned release is
# downloaded once and cached. A wire-breaking pull request pins the matching
# future server release and carries `[wire-break]`, which skips only the jobs
# that resolve this artifact. Local release-candidate validation supplies the
# same server through LASER_TEST_IGGY_SERVER. LASER_TEST_IGGY_VERSION overrides
# the pin and LASER_TEST_IGGY_SHA256, when set, pins the downloaded bytes.
readonly FORK_VERSION="${LASER_TEST_IGGY_VERSION:-0.9.2-ld}"
readonly RELEASES_URL="${LASER_TEST_IGGY_RELEASES_URL:-https://artifacts.laserdata.com}"

if [[ "${1:-}" == "--version" ]]; then
  if [[ $# -ne 1 ]]; then
    echo "usage: $0 [--version]" >&2
    exit 2
  fi
  printf '%s\n' "$FORK_VERSION"
  exit 0
fi

if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--version]" >&2
  exit 2
fi

# LASER_TEST_IGGY_SERVER: Explicit trust override for local development.
# When set, the resolver does not download or verify; it uses the provided binary directly.
# This is an explicit, opt-in override: the user is responsible for validating the binary.
if [[ -n "${LASER_TEST_IGGY_SERVER:-}" ]]; then
  if [[ ! -f "$LASER_TEST_IGGY_SERVER" ]] || [[ ! -x "$LASER_TEST_IGGY_SERVER" ]]; then
    echo "LASER_TEST_IGGY_SERVER is not a regular, executable file: $LASER_TEST_IGGY_SERVER" >&2
    exit 1
  fi
  printf '%s\n' "$LASER_TEST_IGGY_SERVER"
  exit 0
fi

case "$(uname -m)" in
  x86_64)
    suffix="amd64-skylake"
    ;;
  *)
    echo "No $FORK_VERSION test server is published for $(uname -m)." >&2
    echo "Set LASER_TEST_IGGY_SERVER to a compatible local binary." >&2
    exit 1
    ;;
esac

# If a non-default version is requested, require an explicit checksum or LASER_TEST_IGGY_SERVER.
if [[ "$FORK_VERSION" != "0.9.2-ld" ]]; then
  if [[ -z "${LASER_TEST_IGGY_SHA256:-}" ]]; then
    # Check if a version-specific pin file exists (includes FORK_VERSION in filename).
    script_dir="$(dirname "$(realpath "$0")")"
    pin_file="$script_dir/iggy-server-${FORK_VERSION}-${suffix}.sha256"
    if [[ ! -f "$pin_file" ]]; then
      echo "No trusted checksum for version $FORK_VERSION." >&2
      echo "Expected pin file: $pin_file" >&2
      echo "Either provide LASER_TEST_IGGY_SHA256 explicitly or use LASER_TEST_IGGY_SERVER." >&2
      exit 1
    fi
  fi
fi

cache_root="${XDG_CACHE_HOME:-${HOME}/.cache}/laser-sdk/iggy-server/$FORK_VERSION"
binary="$cache_root/iggy-server-linux-$suffix"
mkdir -p "$cache_root"

verify() {
  local file="$1"
  local expected_sha256
  
  if [[ -n "${LASER_TEST_IGGY_SHA256:-}" ]]; then
    # Explicit override: user provided a checksum via environment variable.
    expected_sha256="${LASER_TEST_IGGY_SHA256}"
  else
    # Fall back to the repository-pinned digest.
    local pin_file="$(dirname "$(realpath "$0")")/iggy-server-${suffix}.sha256"
    if [[ ! -f "$pin_file" ]]; then
      echo "No trusted checksum available for iggy-server-${suffix}." >&2
      echo "Set LASER_TEST_IGGY_SHA256 or use LASER_TEST_IGGY_SERVER to specify a local binary." >&2
      return 1
    fi
    # Extract the hash (first field, space-delimited).
    expected_sha256="$(awk '{print $1}' "$pin_file")"
    if [[ -z "$expected_sha256" ]]; then
      echo "Empty checksum in $pin_file" >&2
      return 1
    fi
  fi
  
  # Validate checksum format: must be exactly 64 hex characters.
  if ! [[ "$expected_sha256" =~ ^[0-9a-fA-F]{64}$ ]]; then
    echo "Invalid SHA-256 format: '$expected_sha256' is not 64 hex characters" >&2
    return 1
  fi
  
  # Verify the file against the checksum.
  printf '%s  %s\n' "$expected_sha256" "$file" | sha256sum --check --status
}

if [[ -x "$binary" ]]; then
  if verify "$binary"; then
    printf '%s\n' "$binary"
    exit 0
  else
    # Cached binary failed verification; delete it and re-download.
    echo "Cached binary failed verification: $binary" >&2
    rm -f "$binary"
  fi
fi

temporary="$(mktemp "$cache_root/.download.XXXXXX")"
trap 'rm -f "$temporary"' EXIT
url="$RELEASES_URL/iggy-server/$FORK_VERSION/iggy-server-linux-$suffix"

echo "Downloading Iggy $FORK_VERSION from $url" >&2
curl -fsSL --retry 3 --connect-timeout 10 --max-time 300 "$url" -o "$temporary"

if ! verify "$temporary"; then
  echo "Downloaded binary failed integrity verification; refusing to install." >&2
  exit 1
fi

chmod 755 "$temporary"
mv -f "$temporary" "$binary"
trap - EXIT

printf '%s\n' "$binary"
