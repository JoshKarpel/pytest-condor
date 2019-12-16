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

from typing import List, Tuple

import logging


import os
import subprocess
import re


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DaemonLog:
    def __init__(self, path):
        self.path = path

    def open(self):
        return DaemonLogStream(self.path.open(mode="r", encoding="utf-8"))


class DaemonLogStream:
    def __init__(self, file):
        self.file = file
        self.lines = []

    def __iter__(self):
        yield from self.readlines()

    def readlines(self):
        for line in self.file:
            line = line.strip()
            self.lines.append(line)
            yield line

    def read(self):
        return "\n".join(self.readlines())

    def clear(self):
        """Clear the internal line buffer; useful for isolating tests."""
        self.lines.clear()
