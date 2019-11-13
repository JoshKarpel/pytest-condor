@echo off

SET CONTAINER_TAG=pytest-condor-test

ECHO Building pytest-condor-test testing container...

docker build -t %CONTAINER_TAG% .

docker run -it --rm --mount type=bind,source="%CD%",target=/home/tester/pytest-condor,readonly %CONTAINER_TAG% %*
