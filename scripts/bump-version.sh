#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..

if [ -z "${1:-}" ]; then
    set -- "patch"
fi

if [ "$(git diff --stat 2> /dev/null | grep -v CHANGELOG.md |  sed '$d')" != "" ]; then
    echo "ERROR: There are uncommitted changes in this repository!"
    echo "Please commit all changes before tagging a new version."
    exit 1
fi

VERSION=$(grep '^version' Cargo.toml | cut -d\" -f2 | head -1)
MAJOR=$(echo "$VERSION" | cut -d. -f1)
MINOR=$(echo "$VERSION" | cut -d. -f2)
PATCH=$(echo "$VERSION" | cut -d. -f3)

case $1 in
major)
    TARGET="$(($MAJOR + 1)).0.0"
    ;;
minor)
    TARGET="$MAJOR.$(($MINOR + 1)).0"
    ;;
patch)
    TARGET="$MAJOR.$MINOR.$(($PATCH + 1))"
    ;;
*)
    if ! echo "$1" | grep -Eq '^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$'; then
        echo "Usage: $0 <version | major | minor | patch>"
        echo "ERROR: Version must be a valid semver in format 'x.y.z'"
        exit 1
    fi

    TARGET="$1"
    ;;
esac

echo "Current version: $VERSION"
echo "Bumping version: $TARGET"

# Check that the tag does not exist
if git rev-list "${TARGET}.." &>/dev/null; then
  echo "ERROR: Tag ${TARGET} already exists."
  exit 1
fi

# Check that there's a valid CHANGELOG entry for the new version
UNRELEASED_MARKER="[Unreleased]"
echo "Checking changelog entry..."
CHANGELOG_NEW=$(cat CHANGELOG.md \
                | sed -E -e "1,/^## +(\\$UNRELEASED_MARKER|$TARGET)/ d"  \
                | sed -E -e "/^## /,$ d"                                 \
                | sed -E -e "/^ *$/ d")

if [ -z "${CHANGELOG_NEW}" ]; then
    echo "ERROR: Invalid or empty CHANGELOG entry for version ${TARGET}!"
    echo "ERROR: Put your changes after the unreleased placeholder (${UNRELEASED_MARKER})."
    exit 1
fi
echo "Changelog entry found."

# Replace version in CHANGELOG, add a new "unreleased" section
sed -i '' -E -e "\
s/^(.*)(\\${UNRELEASED_MARKER}|$TARGET)(.*)$\
/\1${UNRELEASED_MARKER}\3\\
\\
\1${TARGET}\3/" CHANGELOG.md

VERSION_RE=${VERSION//\./\\.}
find . -name Cargo.toml -type f -exec sed -i '' -e "1,/^version/ s/^version.*/version = \"$TARGET\"/" {} \;
cargo update -p semaphore

git commit -a -m "release: $TARGET" > /dev/null
git tag "$TARGET"

echo
echo "Updated version and tagged release $TARGET, please run:"
echo " git push origin master $TARGET"
