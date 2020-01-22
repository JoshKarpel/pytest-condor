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
import copy

import pytest

from ornithology import Condor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TESTS_DIR = Path.home() / "tests"

RE_ID = re.compile(r"\[([^()]*)\]$")

ALREADY_SEEN = set()
CONFIG_IDS = collections.defaultdict(set)


class TestDir:
    def __init__(self):
        self._path = None

    def _recompute(self, funcitem):
        dir = TESTS_DIR / funcitem.module.__name__ / funcitem.cls.__name__

        id = RE_ID.search(funcitem.nodeid)

        if id is not None:
            config_ids = CONFIG_IDS[funcitem.module.__name__]
            ids = [id for id in id.group(1).split("-") if id in config_ids]
            if len(ids) > 0:
                dir /= "-".join(ids)

        if dir not in ALREADY_SEEN and dir.exists():
            shutil.rmtree(dir)

        ALREADY_SEEN.add(dir)
        dir.mkdir(parents = True, exist_ok = True)

        self._path = dir

    @property
    def __dict__(self):
        return self._path.__dict__

    def __repr__(self):
        return repr(self._path)

    def __bool__(self):
        try:
            return bool(self._path)
        except RuntimeError:
            return False

    def __dir__(self):
        return dir(self._path)

    def __getattr__(self, name):
        return getattr(self._path, name)

    __str__ = lambda x: str(x._path)
    __lt__ = lambda x, o: x._path < o
    __le__ = lambda x, o: x._path <= o
    __eq__ = lambda x, o: x._path == o
    __ne__ = lambda x, o: x._path != o
    __gt__ = lambda x, o: x._path > o
    __ge__ = lambda x, o: x._path >= o
    __hash__ = lambda x: hash(x._path)
    __len__ = lambda x: len(x._path)
    __getitem__ = lambda x, i: x._path[i]
    __iter__ = lambda x: iter(x._path)
    __contains__ = lambda x, i: i in x._path
    __add__ = lambda x, o: x._path + o
    __sub__ = lambda x, o: x._path - o
    __mul__ = lambda x, o: x._path * o
    __floordiv__ = lambda x, o: x._path // o
    __mod__ = lambda x, o: x._path % o
    __divmod__ = lambda x, o: x._path.__divmod__(o)
    __pow__ = lambda x, o: x._path ** o
    __lshift__ = lambda x, o: x._path << o
    __rshift__ = lambda x, o: x._path >> o
    __and__ = lambda x, o: x._path & o
    __xor__ = lambda x, o: x._path ^ o
    __or__ = lambda x, o: x._path | o
    __truediv__ = lambda x, o: x._path.__truediv__(o)
    __neg__ = lambda x: -(x._path)
    __pos__ = lambda x: +(x._path)
    __abs__ = lambda x: abs(x._path)
    __invert__ = lambda x: ~(x._path)
    __complex__ = lambda x: complex(x._path)
    __int__ = lambda x: int(x._path)
    __float__ = lambda x: float(x._path)
    __oct__ = lambda x: oct(x._path)
    __hex__ = lambda x: hex(x._path)
    __index__ = lambda x: x._path.__index__()
    __coerce__ = lambda x, o: x._path.__coerce__(x, o)
    __enter__ = lambda x: x._path.__enter__()
    __exit__ = lambda x, *a, **kw: x._path.__exit__(*a, **kw)
    __radd__ = lambda x, o: o + x._path
    __rsub__ = lambda x, o: o - x._path
    __rmul__ = lambda x, o: o * x._path
    __rdiv__ = lambda x, o: o / x._path
    __rtruediv__ = __rdiv__
    __rfloordiv__ = lambda x, o: o // x._path
    __rmod__ = lambda x, o: o % x._path
    __rdivmod__ = lambda x, o: x._path.__rdivmod__(o)
    __copy__ = lambda x: copy.copy(x._path)


TEST_DIR = TestDir()


@pytest.fixture(scope = "class")
def test_dir():
    return TEST_DIR


def pytest_runtest_protocol(item, nextitem):
    TEST_DIR._recompute(item)


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


def config(*args, params = None):
    def decorator(func):
        _check_params(params)
        _add_config_ids(func, params)
        return pytest.fixture(
            scope = "module",
            autouse = True,
            params = params.values() if params is not None else None,
            ids = params.keys() if params is not None else None,
        )(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


def standup(*args):
    def decorator(func):
        return pytest.fixture(
            scope = "class",
        )(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


def action(*args, params = None):
    _check_params(params)

    def decorator(func):
        return pytest.fixture(
            scope = "class",
            params = params.values() if params is not None else None,
            ids = params.keys() if params is not None else None,
        )(func)

    if len(args) == 1:
        return decorator(args[0])

    return decorator


@standup
def default_condor(test_dir):
    with Condor(local_dir = test_dir / "condor") as condor:
        yield condor
