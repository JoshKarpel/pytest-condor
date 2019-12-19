#!/usr/bin/env pytest

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from conftest import config, standup, action, get_test_dir

from ornithology import (
    write_file,
    parse_submit_result,
    JobID,
    SetAttribute,
    SetJobStatus,
    JobStatus,
    in_order,
)


class TestJobRouter:
    def test_log(self, default_condor):
        f = default_condor.startd_log.open()

        handle = default_condor.submit({"executable": "/bins/sleep", "arguments": "1"})

        f.wait(lambda msg: "-> Busy" in msg.message)

        f.display_raw()

        assert 0
