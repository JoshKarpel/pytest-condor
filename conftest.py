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

from pathlib import Path
import shutil
import re
import collections

import pytest

from ornithology import Condor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TESTS_DIR = Path.home() / "tests"

RE_ID = re.compile(r"\[([^()]*)\]$")

ALREADY_SEEN = set()
CONFIG_IDS = collections.defaultdict(set)


def get_test_dir(request):
    dir = TESTS_DIR / request.module.__name__

    id = RE_ID.search(request._pyfuncitem.name)

    if id is not None:
        config_ids = CONFIG_IDS[request.module.__name__]
        ids = [id for id in id.group(1).split("-") if id in config_ids]
        logger.debug("ids {}".format(ids))
        if len(ids) > 0:
            dir /= "-".join(ids)

    if dir not in ALREADY_SEEN and dir.exists():
        shutil.rmtree(dir)

    ALREADY_SEEN.add(dir)
    dir.mkdir(parents=True, exist_ok=True)

    logger.debug("dir is {}".format(dir))

    return dir


def _check_params(params):
    if params is None:
        return True

    for key in params.keys():
        if "-" in key:
            raise ValueError('config param keys must not include "-"')


def _add_config_ids(func, params):
    if params is None:
        return

    CONFIG_IDS[func.__module__] |= params.keys()


def config(*args, params=None):
    def decorator(func):
        _check_params(params)
        _add_config_ids(func, params)
        return pytest.fixture(
            scope="module",
            autouse=True,
            params=params.values() if params is not None else None,
            ids=params.keys() if params is not None else None,
        )(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


def standup(*args):
    def decorator(func):
        return pytest.fixture(scope="module")(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


def action(*args, params=None):
    _check_params(params)

    def decorator(func):
        return pytest.fixture(
            scope="class",
            params=params.values() if params is not None else None,
            ids=params.keys() if params is not None else None,
        )(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


@standup
def default_condor(request):
    test_dir = get_test_dir(request)
    with Condor(local_dir=test_dir / "condor") as condor:
        yield condor
