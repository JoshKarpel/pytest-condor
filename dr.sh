#!/usr/bin/env bash

CONTAINER_TAG=pytest-condor-test

set -e

echo "Building pytest-condor-test container..."

docker build -t ${CONTAINER_TAG} --file Dockerfile.centos7 .

echo "Launching pytest-condor-test container..."

docker run -it --rm --mount type=bind,source="$PWD",target=/home/tester/pytest-condor ${CONTAINER_TAG}
