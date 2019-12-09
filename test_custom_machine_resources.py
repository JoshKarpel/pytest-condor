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
        "STARTD_CRON_X_MONITOR_EXECUTABLE": "$(TEST_DIR)/monitor",
        "STARTD_CRON_JOBLIST": "$(STARTD_CRON_JOBLIST) X_MONITOR",
        "STARTD_CRON_X_MONITOR_MODE": "periodic",
        "STARTD_CRON_X_MONITOR_PERIOD": "1",
        "STARTD_CRON_X_MONITOR_METRICS": "SUM:X",
    }
}


# TODO: obviously won't work on windows...
@pytest.fixture(scope="class", params=[(4, [1, 9, 4, 5])])
def num_resources_and_uptimes(request, test_dir):
    num_resources, uptimes = request.param
    ids = [str(i) for i in range(num_resources)]
    names = ["X{}".format(i) for i in ids]

    discovery_script = textwrap.dedent(
        """
        #!/bin/bash
        echo 'DetectedX="{}"'
        exit 0
        """.format(
            ", ".join(names)
        )
    )
    write_file(test_dir / "discovery", discovery_script)

    monitor_script = "#!/bin/bash\n" + "".join(
        textwrap.dedent(
            """
            echo 'SlotMergeConstraint = StringListMember( "{}", AssignedX )'
            echo 'UptimeXSeconds = {}'
            echo '- XSlot{}'
            """.format(
                name, uptime, id
            )
        )
        for name, uptime, id in zip(names, uptimes, ids)
    )
    write_file(test_dir / "monitor", monitor_script)

    logger.debug("Resource discovery script is:{}".format(discovery_script))
    logger.debug("Resource monitor script is:{}".format(monitor_script))

    return num_resources, uptimes


# TODO: obviously won't work in windows...
@pytest.fixture(scope="class", params=[])
def resource_uptimes(request, test_dir):
    return request.param


@pytest.fixture(scope="class", params=SLOT_CONFIGS.values(), ids=SLOT_CONFIGS.keys())
def condor(request, test_dir, num_resources_and_uptimes):
    with Condor(
        local_dir=test_dir / "condor",
        config={**request.param, "TEST_DIR": str(test_dir)},
    ) as condor:
        yield condor


class TestCustomMachineResources:
    def test_correct_number_of_resources_assigned(
        self, condor, num_resources_and_uptimes
    ):
        num_resources, uptimes = num_resources_and_uptimes
        result = condor.status(
            ad_type=htcondor.AdTypes.Startd, projection=["SlotID", "AssignedX"]
        )

        # if a slot doesn't have a resource, it simply has no entry in its ad
        assert len([ad for ad in result if "AssignedX" in ad]) == num_resources

    def test_correct_uptimes_from_monitor(self, condor, num_resources_and_uptimes):
        num_resources, uptimes = num_resources_and_uptimes

        direct = condor.direct_status(
            htcondor.DaemonTypes.Startd,
            htcondor.AdTypes.Startd,
            constraint="AssignedX =!= undefined",
            projection=["SlotID", "AssignedX", "UptimeXSeconds"],
        )

        measured_uptimes = set(int(ad["UptimeXSeconds"]) for ad in direct)

        logger.info(
            "Measured uptimes were {}, expected multiples of {} (not necessarily in order)".format(
                measured_uptimes, uptimes
            )
        )

        # the uptimes are accumulating over time, so we
        # assert that we have some reasonable multiple of the uptimes being
        # emitted by the monitor script
        assert any(
            {multiplier * u for u in uptimes} == measured_uptimes
            for multiplier in range(100)
        )
