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


@pytest.fixture(scope="class")
def handle(test_dir, condor, num_resources_and_uptimes):
    num_resources, _ = num_resources_and_uptimes
    handle = condor.submit(
        description={
            "executable": "/bin/sleep",
            "arguments": "3",
            "request_X": "1",
            "log": (test_dir / "events.log").as_posix(),
            "LeaveJobInQueue": "true",
        },
        count=num_resources * 2,
    )

    # we must wait for both the handle and the job queue here,
    # because we want to use both later
    handle.wait(timeout=60)
    condor.job_queue.wait_for_job_completion(handle.job_ids)

    return handle


@pytest.fixture(scope="class")
def num_jobs_running_history(condor, handle):
    num_running = 0
    num_running_history = []
    for jobid, event in condor.job_queue.filter(lambda j, e: j in handle.job_ids):
        if event == SetJobStatus(JobStatus.RUNNING):
            num_running += 1
        elif event == SetJobStatus(JobStatus.COMPLETED):
            num_running -= 1

        num_running_history.append(num_running)

    return num_running_history


@pytest.fixture(scope="class")
def startd_log_file(condor):
    with condor.startd_log.path.open(mode="r") as f:
        yield f


@pytest.fixture(scope="class")
def num_busy_slots_history(startd_log_file, handle, num_resources_and_uptimes):
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
            for multiplier in range(1000)
        )

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

    def test_reported_usage_in_jobads_and_event_log_match(self, handle):
        terminated_events = handle.event_log.filter(
            lambda e: e.type is htcondor.JobEventType.JOB_TERMINATED
        )
        jobid_to_usage_via_event = {
            JobID.from_job_event(event): event["XUsage"]
            for event in sorted(terminated_events, key=lambda e: e.proc)
        }

        ads = handle.query(
            projection=["ClusterID", "ProcID", "XUsage", "XAverageUsage"]
        )
        jobid_to_usage_via_ad = {
            JobID.from_job_ad(ad): round(ad["XUsage"], 2)
            for ad in sorted(ads, key=lambda ad: ad["ProcID"])
        }

        logger.debug(
            "Custom resource usage from job event log: {}".format(
                jobid_to_usage_via_event
            )
        )
        logger.debug("Custom resource from job ads: {}".format(jobid_to_usage_via_ad))

        # make sure we got the right number of ads and terminate events
        # before doing the real assertion
        assert (
            len(jobid_to_usage_via_event) == len(jobid_to_usage_via_ad) == len(handle)
        )

        assert jobid_to_usage_via_ad == jobid_to_usage_via_event
