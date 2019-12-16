#!/usr/bin/env pytest

# this test replicates cmr-monitor-basic

import logging

import textwrap
import fractions

import pytest

import htcondor

from ornithology import (
    Condor,
    write_file,
    JobID,
    SetJobStatus,
    JobStatus,
    track_quantity,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

MONITOR_PERIOD = 5
NUM_PERIODS = 3

# the custom resource is named X
SLOT_CONFIGS = {
    "static_slots": {
        "NUM_CPUS": "16",
        "NUM_SLOTS": "16",
        "MACHINE_RESOURCE_INVENTORY_X": "$(TEST_DIR)/discovery",
        "STARTD_CRON_X_MONITOR_EXECUTABLE": "$(TEST_DIR)/monitor",
        "STARTD_CRON_JOBLIST": "$(STARTD_CRON_JOBLIST) X_MONITOR",
        "STARTD_CRON_X_MONITOR_MODE": "periodic",
        "STARTD_CRON_X_MONITOR_PERIOD": str(MONITOR_PERIOD),
        "STARTD_CRON_X_MONITOR_METRICS": "SUM:X",
    }
}

RESOURCES_AND_INCREMENTS = [{"X0": 5, "X1": 5, "X2": 5, "X3": 5}]


# TODO: obviously won't work on windows...
@pytest.fixture(scope="class", params=RESOURCES_AND_INCREMENTS)
def resources(request, test_dir):
    resources = request.param

    discovery_script = textwrap.dedent(
        """
        #!/bin/bash
        echo 'DetectedX="{}"'
        exit 0
        """.format(
            ", ".join(resources.keys())
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
                name, incremenent, name.lstrip("X")
            )
        )
        for name, incremenent in resources.items()
    )
    write_file(test_dir / "monitor", monitor_script)

    logger.debug("Resource discovery script is:{}".format(discovery_script))
    logger.debug("Resource monitor script is:{}".format(monitor_script))

    return resources


@pytest.fixture(scope="class")
def num_resources(resources):
    return len(resources)


@pytest.fixture(scope="class", params=SLOT_CONFIGS.values(), ids=SLOT_CONFIGS.keys())
def condor(request, test_dir, resources):
    with Condor(
        local_dir=test_dir / "condor",
        config={**request.param, "TEST_DIR": str(test_dir)},
    ) as condor:
        yield condor


@pytest.fixture(scope="class")
def handle(test_dir, condor, num_resources):
    handle = condor.submit(
        description={
            "executable": "/bin/sleep",
            "arguments": "17",
            "request_X": "1",
            "log": (test_dir / "events.log").as_posix(),
            "LeaveJobInQueue": "true",
            "job_machine_attrs": "AssignedX",
        },
        count=num_resources * 2,
    )

    # we must wait for both the handle and the job queue here,
    # because we want to use both later
    handle.wait(timeout=60)
    condor.job_queue.wait_for_job_completion(handle.job_ids)

    return handle


@pytest.fixture(scope="class")
def num_jobs_running_history(condor, handle, num_resources):
    return track_quantity(
        condor.job_queue.filter(lambda j, e: j in handle.job_ids),
        increment_condition=lambda id_event: id_event[-1]
        == SetJobStatus(JobStatus.RUNNING),
        decrement_condition=lambda id_event: id_event[-1]
        == SetJobStatus(JobStatus.COMPLETED),
        max_quantity=num_resources,
        expected_quantity=num_resources,
    )


@pytest.fixture(scope="class")
def startd_log_file(condor):
    return condor.startd_log.open()


@pytest.fixture(scope="class")
def num_busy_slots_history(startd_log_file, handle, num_resources):
    logger.debug("Checking Startd log file...")
    logger.debug("Expected Job IDs are:", handle.job_ids)

    startd_log_file.read()

    active_claims_history = track_quantity(
        startd_log_file.lines,
        increment_condition=lambda line: "Changing activity: Idle -> Busy" in line,
        decrement_condition=lambda line: "Changing activity: Busy -> Idle" in line,
        max_quantity=num_resources,
        expected_quantity=num_resources,
    )

    startd_log_file.clear()

    return active_claims_history


class TestCustomMachineResources:
    def test_correct_number_of_resources_assigned(self, condor, num_resources):
        result = condor.status(
            ad_type=htcondor.AdTypes.Startd, projection=["SlotID", "AssignedX"]
        )

        # if a slot doesn't have a resource, it simply has no entry in its ad
        assert len([ad for ad in result if "AssignedX" in ad]) == num_resources

    def test_correct_uptimes_from_monitor(self, condor, resources):
        direct = condor.direct_status(
            htcondor.DaemonTypes.Startd,
            htcondor.AdTypes.Startd,
            constraint="AssignedX =!= undefined",
            projection=["SlotID", "AssignedX", "UptimeXSeconds"],
        )

        measured_uptimes = set(int(ad["UptimeXSeconds"]) for ad in direct)

        logger.info(
            "Measured uptimes were {}, expected multiples of {} (not necessarily in order)".format(
                measured_uptimes, resources.values()
            )
        )

        # the uptimes are increasing over time, so we
        # assert that we have some reasonable multiple of the increments being
        # emitted by the monitor script
        assert any(
            {multiplier * u for u in resources.values()} == measured_uptimes
            for multiplier in range(1000)
        )

    def test_never_more_jobs_running_than_num_resources(
        self, num_jobs_running_history, num_resources
    ):
        assert max(num_jobs_running_history) <= num_resources

    def test_num_jobs_running_hits_num_resources(
        self, num_jobs_running_history, resources
    ):
        num_resources = len(resources)
        assert num_resources in num_jobs_running_history

    def test_never_more_busy_slots_than_num_resources(
        self, num_busy_slots_history, num_resources
    ):
        assert max(num_busy_slots_history) <= num_resources

    def test_num_busy_slots_hits__num_resources(
        self, num_busy_slots_history, num_resources
    ):
        assert num_resources in num_busy_slots_history

    def test_reported_usage_in_job_ads_and_event_log_match(self, handle):
        terminated_events = handle.event_log.filter(
            lambda e: e.type is htcondor.JobEventType.JOB_TERMINATED
        )
        ads = handle.query(projection=["ClusterID", "ProcID", "XUsage"])

        # make sure we got the right number of terminate events and ads
        # before doing the real assertion
        assert len(terminated_events) == len(ads) == len(handle)

        jobid_to_usage_via_event = {
            JobID.from_job_event(event): event["XUsage"]
            for event in sorted(terminated_events, key=lambda e: e.proc)
        }

        jobid_to_usage_via_ad = {
            JobID.from_job_ad(ad): round(ad["XUsage"], 2)
            for ad in sorted(ads, key=lambda ad: ad["ProcID"])
        }

        logger.debug(
            "Custom resource usage from job event log: {}".format(
                jobid_to_usage_via_event
            )
        )
        logger.debug(
            "Custom resource usage from job ads: {}".format(jobid_to_usage_via_ad)
        )

        assert jobid_to_usage_via_ad == jobid_to_usage_via_event

    def test_reported_usage_in_job_ads_makes_sense(self, handle, resources):
        ads = handle.query(
            projection=[
                "ClusterID",
                "ProcID",
                "XUsage",
                "MachineAttrAssignedX0",
                "RemoteWallClockTime",
            ]
        )

        # here's the deal: XUsage is

        #   (increment amount * number of periods)
        # -----------------------------------------
        #     (monitor period * number of periods)

        # BUT in practice, you usually get the monitor period wrong by a second due to rounding
        # what we observe is that very often, some of increments will be a second longer or shorter
        # than the increment period. So we could get something like

        #          (increment amount * number of periods)
        # ---------------------------------------------------------
        # (monitor period * number of periods) + (number of periods)

        # Also, we could get one more increment than expected
        # (which only matters if we also got the long periods; otherwise, it just cancels out).
        # This gives us three kinds of possibilities to check against.

        all_options = []
        for ad in ads:
            increment = resources[ad["MachineAttrAssignedX0"]]
            usage = fractions.Fraction(float(ad["XUsage"])).limit_denominator(30)
            print(
                "Job {}.{}, resource {}, increment {}, usage {} ({})".format(
                    ad["ClusterID"],
                    ad["ProcID"],
                    ad["MachineAttrAssignedX0"],
                    increment,
                    usage,
                    float(usage),
                )
            )

            exact = [fractions.Fraction(increment, MONITOR_PERIOD)]
            dither_periods = [
                fractions.Fraction(
                    increment * NUM_PERIODS,
                    ((MONITOR_PERIOD * NUM_PERIODS) + extra_periods),
                )
                for extra_periods in range(-NUM_PERIODS, NUM_PERIODS + 1)
            ]
            extra_period = [
                fractions.Fraction(
                    increment * NUM_PERIODS + 1,
                    ((MONITOR_PERIOD * (NUM_PERIODS + 1)) + extra_periods),
                )
                for extra_periods in range(-(NUM_PERIODS + 1), NUM_PERIODS + 2)
            ]

            print("*" if usage in exact else " ", "exact".ljust(25), exact)
            print(
                "*" if usage in dither_periods else " ",
                "dither periods".ljust(25),
                dither_periods,
            )
            print(
                "*" if usage in extra_period else " ",
                "dither, extra increment".ljust(25),
                extra_period,
            )
            print()

            # build the list of possibilities here, but delay assertions until we've printed all the debug messages
            all_options.append(exact + dither_periods + extra_period)

        assert all(
            fractions.Fraction(float(ad["XUsage"])).limit_denominator(30) in options
            for ad, options in zip(ads, all_options)
        )
