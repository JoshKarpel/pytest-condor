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

from pathlib import Path
import shutil
import re

import pytest

from ornithology import Condor

TESTS_DIR = Path.home() / "tests"

RE_ID = re.compile(r"\[(.*)\]$")


def get_test_dir(request):
    dir = TESTS_DIR / request.module.__name__

    id = RE_ID.search(request._pyfuncitem.name)
    if id is not None:
        dir /= id.group(1)

    return dir


@pytest.fixture(scope="module")
def test_dir():
    pass


def pytest_fixture_setup(fixturedef, request):
    if fixturedef.argname == "test_dir":
        d = get_test_dir(request)
        fixturedef.cached_result = (d, None, None)
        return d


def config(*args, **kwargs):
    def decorator(func):
        return pytest.fixture(scope="module", autouse=True, **kwargs)(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


def standup(*args, **kwargs):
    def decorator(func):
        return pytest.fixture(scope="module", **kwargs)(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


def action(*args, **kwargs):
    def decorator(func):
        return pytest.fixture(scope="class", **kwargs)(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator
