# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# this dockerfile builds a test environment for watch

FROM ubuntu:bionic

# switch to root to do root-level config
USER root

# build config
ARG HTCONDOR_VERSION=8.9

# environment setup
ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US.UTF-8

# install utils and dependencies
RUN apt-get update \
 && apt-get -y install --no-install-recommends sudo vim less build-essential git gnupg wget ca-certificates locales \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/* \
 && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
 && locale-gen


# install HTCondor version specified in config
RUN wget -qO - https://research.cs.wisc.edu/htcondor/ubuntu/HTCondor-Release.gpg.key | apt-key add - \
 && echo "deb http://research.cs.wisc.edu/htcondor/ubuntu/${HTCONDOR_VERSION}/bionic bionic contrib" >> /etc/apt/sources.list \
 && apt-get -y update \
 && apt-get -y install htcondor \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
 && apt-get -y install --no-install-recommends strace python3-pip \
 && apt-get -y clean

RUN pip3 install --no-cache-dir pytest

# create a user to be our submitter and set conda install location
ENV USER=tester
RUN groupadd ${USER} \
 && useradd -m -g ${USER} ${USER}

# switch to submit user, don't need root anymore
USER ${USER}

CMD ["bash"]
WORKDIR /home/tester/pytest-condor
