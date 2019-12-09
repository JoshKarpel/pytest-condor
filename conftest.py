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

import pytest

from ornithology import Condor


TESTS_DIR = Path.home() / "tests"


@pytest.fixture(scope="class")
def test_dir(request):
    if request.cls is not None:
        dir = TESTS_DIR / request.cls.__name__
    else:
        dir = TESTS_DIR / request.function.__name__

    return dir


@pytest.fixture(scope="class")
def default_condor(test_dir):
    with Condor(local_dir=test_dir / "condor") as condor:
        yield condor
