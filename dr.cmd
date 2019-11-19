@echo off

SET CONTAINER_TAG=pytest-condor-test

ECHO Building pytest-condor-test container...

docker build -t %CONTAINER_TAG% .

ECHO Launching pytest-condor-test container...

docker run -it --rm --mount type=bind,source="%CD%",target=/home/tester/pytest-condor %CONTAINER_TAG% %*
