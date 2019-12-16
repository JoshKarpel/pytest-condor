#!/usr/bin/env pytest

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import pytest

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
        f = default_condor.master_log.open()

        f.read()

        for msg in f.messages:
            # print(msg)
            print(repr(msg))

        assert 0
