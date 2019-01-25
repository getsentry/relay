#!/bin/bash
### Build and push the Docker image to Dockerhub, but only when building "master".
set -eux

if [ "$TRAVIS_BRANCH" != "master" -o "$TRAVIS_PULL_REQUEST" != "false" ]; then
  echo "Not pushing the image to Dockerhub"
  exit 0
fi

echo "$DOCKER_PASS" | docker login -u "$DOCKER_USER" --password-stdin
cp "target/${BUILD_ARCH}-unknown-linux-gnu/release/semaphore" semaphore
zip semaphore-debug.zip "target/${BUILD_ARCH}-unknown-linux-gnu/release/semaphore.debug"

docker pull "${IMAGE_NAME}" || true
docker build --pull --cache-from "${IMAGE_NAME}" -f Dockerfile.publish --tag "${IMAGE_NAME}" .

docker tag "${IMAGE_NAME}" "${IMAGE_NAME}:latest"
docker tag "${IMAGE_NAME}" "${IMAGE_NAME}:$(git rev-parse HEAD)"

docker push "${IMAGE_NAME}:latest"
docker push "${IMAGE_NAME}:$(git rev-parse HEAD)"
