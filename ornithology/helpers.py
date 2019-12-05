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

from typing import Any, Iterable

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import itertools


def in_order(items: Iterable[Any], expected) -> bool:
    """
    Given an iterable of items and a list of expected items, return ``True``
    if and only if the items occur in exactly the given order. Extra items may
    appear between expected items, but expected items may not appear out of order.

    When this function returns ``False``, it emits a detailed log message at
    ERROR level showing a "match" display for debugging.

    Parameters
    ----------
    items
    expected

    Returns
    -------

    """
    items = iter(items)
    items, backup = itertools.tee(items)

    expected = list(expected)
    expected_set = set(expected)
    next_expected_idx = 0

    found_at = {}

    for found_idx, item in enumerate(items):
        if item not in expected_set:
            continue

        if item == expected[next_expected_idx]:
            found_at[found_idx] = expected[next_expected_idx]
            next_expected_idx += 1

            if next_expected_idx == len(expected):
                # we have seen all the expected events
                return True
        else:
            break

    backup = list(backup)
    msg_lines = ["Items were not in the correct order:"]
    for idx, item in enumerate(backup):
        msg = " {:6}  {}".format(idx, item)

        maybe_found = found_at.get(idx)
        if maybe_found is not None:
            msg = "*" + msg[1:]
            msg += "  MATCHED  {}".format(maybe_found)

        msg_lines.append(msg)

    logger.error("\n".join(msg_lines))
    return False
