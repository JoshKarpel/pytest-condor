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

import os
from pathlib import Path

import htcondor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def set_env_var(key: str, value: str):
    os.environ[key] = value
    logger.debug("Set environment variable {} = {}".format(key, value))


def unset_env_var(key: str):
    value = os.environ.get(key, None)

    if value is not None:
        del os.environ[key]
        logger.debug("Unset environment variable {}, value was {}".format(key, value))


class SetEnv:
    """
    A context manager. Inside the block, the Condor config file is the one given
    to the constructor. After the block, it is reset to whatever it was before
    the block was entered.

    If you need to change the ``CONDOR_CONFIG``, use the specialized
    :func:`SetCondorConfig`.
    """

    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value
        self.previous_value = None

    def __enter__(self):
        self.previous_value = os.environ.get(self.key, None)
        set_env_var(self.key, self.value)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.previous_value is not None:
            set_env_var(self.key, self.previous_value)
        else:
            unset_env_var(self.key)


class SetCondorConfig:
    """
    A context manager. Inside the block, the Condor config file is the one given
    to the constructor. After the block, it is reset to whatever it was before
    the block was entered.
    """

    def __init__(self, config_file: Path):
        self.config_file = Path(config_file)
        self.previous_value = None

    def __enter__(self):
        self.previous_value = os.environ.get("CONDOR_CONFIG", None)
        set_env_var("CONDOR_CONFIG", self.config_file.as_posix())

        htcondor.reload_config()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.previous_value is not None:
            set_env_var("CONDOR_CONFIG", self.previous_value)
        else:
            unset_env_var("CONDOR_CONFIG")

        htcondor.reload_config()


class ChangeDir:
    """
    A context manager. Inside the block, the current working directory is changed
    to whatever is given to the constructor. After the block, it is reset to
    where it was when the block was entered.
    """

    def __init__(self, dir: Path):
        self.dir = dir
        self.previous_dir = None

    def __enter__(self):
        self.previous_dir = Path.cwd()
        os.chdir(self.dir)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.previous_dir)
