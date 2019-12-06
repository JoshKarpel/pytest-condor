#!/usr/bin/env pytest

# this test replicates cmr-monitor-basic

import logging

import textwrap

import pytest

from ornithology import Condor, write_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SLOT_CONFIGS = {
    "static_slots": {
        "NUM_CPUS": "16",
        "NUM_SLOTS": "16",
        "MACHINE_RESOURCE_INVENTORY_SQUIDs": "$(TEST_DIR)/cmr-squid-discovery",
        "STARTD_CRON_JOBLIST": "$(STARTD_CRON_JOBLIST) SQUIDs_MONITOR",
        "STARTD_CRON_SQUIDs_MONITOR_MODE": "periodic",
        "STARTD_CRON_SQUIDs_MONITOR_PERIOD": "10",
        "STARTD_CRON_SQUIDs_MONITOR_EXECUTABLE": "$(TEST_DIR)/cmr-squid-monitor",
        "STARTD_CRON_SQUIDs_MONITOR_METRICS": "SUM:SQUIDs",
        "SCHEDD_CLUSTER_INITIAL_VALUE": "1000",
    }
}


@pytest.fixture(scope="class", params=SLOT_CONFIGS.values(), ids=SLOT_CONFIGS.keys())
def condor(request, test_dir):
    write_file(
        test_dir / "cmr-squid-discovery",
        textwrap.dedent(
            """
            #!/bin/bash
            echo 'DetectedSQUIDs="SQUID0, SQUID1, SQUID2, SQUID3"'
            exit 0
            """
        ),
    )

    with Condor(
        local_dir=test_dir / "condor",
        config={**request.param, "TEST_DIR": str(test_dir)},
    ) as condor:

        yield condor


class TestCustomMachineResources:
    def test_slot_and_squid_count(self, condor):
        condor.run_command(["condor_status"])
        condor.run_command(["condor_status", "-af", "AssignedSQUIDs"])
        assert 0
