#!/bin/bash
# Bump "marshal" version in common/
#
# Usage:
#     bump-marshal.sh MARSHAL_GIT_SHA [--no-commit]
#
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..

COMMIT="1"

while [[ $# -gt 0 ]]; do
    case "${1}" in
    -n|--no-commit)
        COMMIT=""
        shift
        ;;
    *)
        MARSHAL_SHA="${1}"
        shift
        ;;
    esac
done

sed -i '' -e "s|^\(marshal.*github.com/getsentry/marshal\", rev = \)\"[a-z0-9]*\"|\1\"${MARSHAL_SHA}\"|" \
    common/Cargo.toml

cargo update -p marshal --precise $MARSHAL_SHA
cargo update -p marshal --precise $MARSHAL_SHA --manifest-path ./cabi/Cargo.toml

if [ -n "${COMMIT}" ]; then
    # Commit changes
    git commit --all -n -m "build: Bump getsentry/marshal@${MARSHAL_SHA}"
fi
