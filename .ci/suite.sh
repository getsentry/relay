#!/bin/sh

if [ "$SUITE" == "test" ]; then
  cargo test --all
elif [ "$SUITE" == "format" ]; then
  cargo fmt -- --write-mode diff
else
  echo 'error: unknown suite'
  exit 1
fi
