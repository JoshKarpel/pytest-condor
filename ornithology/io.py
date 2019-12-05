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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


import textwrap
from pathlib import Path


def write_file(path: Path, text: str) -> Path:
    """
    Write the given ``text`` to a new file at the given ``path``, stomping
    anything that might exist there.

    Parameters
    ----------
    path
    text

    Returns
    -------
    path
        The path the file was written to (as an absolute path).
    """
    path = Path(path).absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path
