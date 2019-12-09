#!/usr/bin/env pytest

# this test replicates cmr-monitor-basic

import logging

import textwrap

import pytest

import htcondor

from ornithology import Condor, write_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# the custom resource is named X
SLOT_CONFIGS = {
    "static_slots": {
        "NUM_CPUS": "16",
        "NUM_SLOTS": "16",
        "MACHINE_RESOURCE_INVENTORY_X": "$(TEST_DIR)/discovery",
        "STARTD_CRON_JOBLIST": "$(STARTD_CRON_JOBLIST) X_MONITOR",
        "STARTD_CRON_X_MONITOR_MODE": "periodic",
        "STARTD_CRON_X_MONITOR_PERIOD": "10",
        "STARTD_CRON_X_MONITOR_EXECUTABLE": "$(TEST_DIR)/monitor",
        "STARTD_CRON_X_MONITOR_METRICS": "SUM:X",
    }
}


# TODO: obviously won't work in windows...
@pytest.fixture(scope="class", params=[4])
def num_resources(request, test_dir):
    write_file(
        test_dir / "discovery",
        textwrap.dedent(
            """
            #!/bin/bash
            echo 'DetectedX="{}"'
            exit 0
            """.format(
                ", ".join("X-{}".format(i) for i in range(request.param))
            )
        ),
    )

    return request.param


@pytest.fixture(scope="class", params=SLOT_CONFIGS.values(), ids=SLOT_CONFIGS.keys())
def condor(request, test_dir, num_resources):
    with Condor(
        local_dir=test_dir / "condor",
        config={**request.param, "TEST_DIR": str(test_dir)},
    ) as condor:
        yield condor


class TestCustomMachineResources:
    def test_correct_number_of_resources_assigned(self, condor, num_resources):
        query = condor.status(ad_type=htcondor.AdTypes.Startd, projection=["AssignedX"])

        logger.debug(
            "Ads returned by status query:\n" + "\n".join(str(ad) for ad in query)
        )

        # if a slot doesn't have a resource, it simply has no entry in its ad
        assert len([ad for ad in query if "AssignedX" in ad]) == num_resources
