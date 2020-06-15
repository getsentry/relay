#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..

OLD_VERSION="${1}"
NEW_VERSION="${2}"

echo "Current version: ${OLD_VERSION}"
echo "Bumping version: ${NEW_VERSION}"

VERSION_RE=${OLD_VERSION//\./\\.}
find . -name Cargo.toml -not -path './relay-cabi/*' -exec sed -i.bak -e "1,/^version/ s/^version.*/version = \"${NEW_VERSION}\"/" {} \;
find . -name Cargo.toml.bak -exec rm {} \;

cargo update -p relay
