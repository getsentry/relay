#!/bin/bash
set -euo pipefail

if [ "$(uname -s)" != "Linux" ]; then
    echo "Sentry Lambda Extension can only be released on Linux!"
    echo "Please use the GitHub Action instead."
    exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..

OLD_VERSION="${1}"
NEW_VERSION="${2}"

echo "Current version: ${OLD_VERSION}"
echo "Bumping version: ${NEW_VERSION}"

TOML_FILES="$(git ls-files 'Cargo.toml' | grep -v cabi)"
perl -pi -e "s/^version = .*\$/version = \"$NEW_VERSION\"/" $TOML_FILES

cargo update -p sentry-lambda-extension

CHANGE_DATE="$(date +'%Y-%m-%d' -d '3 years')"
echo "Bumping Change Date to $CHANGE_DATE"
sed -i -e "s/\(Change Date:\s*\)[-0-9]\+\$/\\1$CHANGE_DATE/" LICENSE
