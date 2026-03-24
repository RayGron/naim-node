#!/usr/bin/env bash
set -euo pipefail

uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "${uname_s}" in
  Linux)
    os="linux"
    ;;
  Darwin)
    os="macos"
    ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT)
    os="windows"
    ;;
  *)
    echo "error: unsupported host OS '${uname_s}'" >&2
    exit 1
    ;;
esac

case "${uname_m}" in
  x86_64|amd64)
    arch="x64"
    ;;
  aarch64|arm64)
    arch="arm64"
    ;;
  *)
    echo "error: unsupported host architecture '${uname_m}'" >&2
    exit 1
    ;;
esac

printf '%s %s\n' "${os}" "${arch}"
