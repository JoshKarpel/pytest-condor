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

import logging

import enum

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class JobID:
    def __init__(self, cluster, proc):
        self.cluster = int(cluster)
        self.proc = int(proc)

    def __eq__(self, other):
        return (
            (isinstance(other, self.__class__) or isinstance(self, other.__class__))
            and self.cluster == other.cluster
            and self.proc == other.proc
        )

    def __hash__(self):
        return hash((self.__class__, self.cluster, self.proc))

    def __repr__(self):
        return "{}(cluster = {}, proc = {})".format(
            self.__class__.__name__, self.cluster, self.proc
        )

    def __str__(self):
        return "{}.{}".format(self.cluster, self.proc)


class JobStatus(str, enum.Enum):
    Idle = "1"
    Running = "2"
    Removed = "3"
    Completed = "4"
    Held = "5"
    TransferringOutput = "6"
    Suspended = "7"

    def __repr__(self):
        return "{}.{}".format(self.__class__.__name__, self.name)

    __str__ = __repr__
