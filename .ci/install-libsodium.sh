#!/bin/bash

if [ "$TRAVIS_OS_NAME" == "osx" ]; then
  brew update
  brew install libsodium
else
  sudo add-apt-repository ppa:chris-lea/libsodium -y
  sudo apt-get update && sudo apt-get install libsodium-dev -y
fi
