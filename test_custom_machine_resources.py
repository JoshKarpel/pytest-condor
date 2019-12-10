#!/usr/bin/env pytest

# this test replicates cmr-monitor-basic

import logging

import textwrap

import pytest

import htcondor

from ornithology import Condor, write_file, JobID, SetJobStatus, JobStatus

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


@pytest.fixture(scope="class", params=SLOT_CONFIGS.values(), ids=SLOT_CONFIGS.keys())
def condor(request, test_dir, num_resources_and_uptimes):
    with Condor(
        local_dir=test_dir / "condor",
        config={**request.param, "TEST_DIR": str(test_dir)},
    ) as condor:
        yield condor


class TestCustomMachineResourcesInternalBehavior:
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
            for multiplier in range(1000)
        )


@pytest.fixture(scope="class")
def jobids(condor):
    result = condor.submit(
        description={"executable": "/bin/sleep", "arguments": "5", "request_X": "1"},
        count=5,
    )

    jobids = [JobID(result.cluster(), proc) for proc in range(result.num_procs())]

    condor.job_queue.wait(
        expected_events={jobid: [SetJobStatus(JobStatus.Completed)] for jobid in jobids}
    )

    return jobids


@pytest.fixture(scope="class")
def num_jobs_running_history(condor, jobids):
    num_running = 0
    num_running_history = []
    for jobid, event in condor.job_queue.filter(lambda j, e: j in jobids):
        if event == SetJobStatus(JobStatus.Running):
            num_running += 1
        elif event == SetJobStatus(JobStatus.Completed):
            num_running -= 1

        num_running_history.append(num_running)

    return num_running_history


@pytest.fixture(scope="class")
def startd_log_file(condor):
    with condor.startd_log.path.open(mode="r") as f:
        yield f


@pytest.fixture(scope="class")
def num_busy_slots_history(startd_log_file, jobids, num_resources_and_uptimes):
    num_resources, _ = num_resources_and_uptimes

    active_claims_history = []
    active_claims = 0

    for line in startd_log_file:
        line = line.strip()

        if "Changing activity: Idle -> Busy" in line:
            active_claims += 1
        elif "Changing activity: Busy -> Idle" in line:
            active_claims -= 1
        active_claims_history.append(active_claims)

        print(
            "{} {}/{} | {}".format(
                "*"
                if len(active_claims_history) > 2
                and active_claims_history[-1] != active_claims_history[-2]
                else " ",
                str(active_claims).rjust(2),
                num_resources,
                line,
            )
        )

    return active_claims_history


class TestTestCustomMachineResourcesUserVisibleBehavior:
    def test_never_more_jobs_running_than_num_resources(
        self, num_jobs_running_history, num_resources_and_uptimes
    ):
        num_resources, _ = num_resources_and_uptimes
        assert max(num_jobs_running_history) <= num_resources

    def test_num_jobs_running_hits_num_resources(
        self, num_jobs_running_history, num_resources_and_uptimes
    ):
        num_resources, _ = num_resources_and_uptimes
        assert num_resources in num_jobs_running_history

    def test_never_more_busy_slots_than_num_resources(
        self, num_busy_slots_history, num_resources_and_uptimes
    ):
        num_resources, _ = num_resources_and_uptimes
        assert max(num_busy_slots_history) <= num_resources

    def test_num_busy_slots_hits__num_resources(
        self, num_busy_slots_history, num_resources_and_uptimes
    ):
        num_resources, _ = num_resources_and_uptimes
        assert num_resources in num_busy_slots_history
