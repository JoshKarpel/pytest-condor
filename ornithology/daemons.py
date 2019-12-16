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


import re
import datetime

from . import exceptions


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
        self.messages = []

    @property
    def lines(self):
        yield from (msg.line for msg in self.messages)

    def readlines(self):
        for line in self.file:
            line = line.strip()
            self.messages.append(LogMessage(line))
            yield line

    def read(self):
        return "\n".join(self.readlines())

    def clear(self):
        """Clear the internal message store; useful for isolating tests."""
        self.messages.clear()


RE_MESSAGE = re.compile(
    r"^(?P<timestamp>\d{2}\/\d{2}\/\d{2}\s\d{2}:\d{2}:\d{2})\s(?P<msg>.*)$"
)

LOG_MESSAGE_TIME_FORMAT = r"%m/%d/%y %H:%M:%S"


class LogMessage:
    def __init__(self, line):
        self.line = line
        match = RE_MESSAGE.match(line)
        if match is None:
            raise exceptions.DaemonLogParsingFailed(
                "Failed to parse daemon log line: {}".format(line)
            )

        self.timestamp = datetime.datetime.strptime(
            match.group("timestamp"), LOG_MESSAGE_TIME_FORMAT
        )

        msg = match.group("msg")
        tags, message = self._parse_msg(msg)
        self.tags = tags
        self.message = message

    def _parse_msg(self, msg):
        tags = []
        inside = False
        message = ""

        msg = iter(msg)
        for char in msg:
            if char == "(":
                tag = []
                inside = True
            elif char == ")":
                tags.append("".join(tag))
                inside = False
            elif inside:
                tag.append(char)
            elif not inside and char == " ":
                continue
            else:
                message = char
                break

        return tags, message + "".join(msg)

    def __str__(self):
        return self.line

    def __repr__(self):
        return 'LogMessage(timestamp = {}, tags = {}, message = "{}")'.format(
            self.timestamp, self.tags, self.message
        )
