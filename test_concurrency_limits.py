#!/usr/bin/env pytest

# this test replicates job_concurrency_limitsP

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import pytest

from ornithology import (
    Condor,
    write_file,
    parse_submit_result,
    JobID,
    SetJobStatus,
    JobStatus,
    in_order,
    track_quantity,
)

# the effective number of slots should always be much larger than the number of
# jobs you plan to submit
SLOT_CONFIGS = {
    "static_slots": {"NUM_CPUS": "12", "NUM_SLOTS": "12"},
    "partitionable_slot": {
        "NUM_CPUS": "12",
        "SLOT_TYPE_1": "cpus=100%,memory=100%,disk=100%",
        "SLOT_TYPE_1_PARTITIONABLE": "True",
        "NUM_SLOTS_TYPE_1": "1",
    },
}


@pytest.fixture(scope="class", params=SLOT_CONFIGS.items(), ids=SLOT_CONFIGS.keys())
def condor(request, test_dir):
    config_name, config = request.param
    with Condor(
        local_dir=test_dir / "condor-{}".format(config_name),
        config={
            **config,
            # make the sure the negotiator runs many times within a single job duration
            "NEGOTIATOR_INTERVAL": "1",
            # below are the concurrency limits we'll test against
            # if you change these, change the below fixture as well
            "XSW_LIMIT": "4",
            "CONCURRENCY_LIMIT_DEFAULT": "2",
            "CONCURRENCY_LIMIT_DEFAULT_SMALL": "3",
            "CONCURRENCY_LIMIT_DEFAULT_LARGE": "1",
        },
    ) as condor:
        yield condor


@pytest.fixture(
    scope="class",
    # these should match the limits expressed in the config
    params=[("XSW", 4), ("UNDEFINED:2", 1), ("small.license", 3), ("large.license", 1)],
    ids=["named_limit", "default_limit", "default_small", "default_large"],
)
def concurrency_limits_and_max_running(request):
    return request.param


@pytest.fixture(scope="class")
def jobids_for_sleep_jobs(test_dir, condor, concurrency_limits_and_max_running):
    cl, mr = concurrency_limits_and_max_running

    # we need the non-zero sleep to make sure we hit the concurrency limit
    sub_description = """
        executable = /bin/sleep
        arguments = 5
        
        request_memory = 1MB
        request_disk = 1MB
        
        concurrency_limits = {concurrency_limits}
        
        queue {num_jobs}
    """.format(
        concurrency_limits=cl, num_jobs=(mr + 1) * 2
    )
    submit_file = write_file(test_dir / "submit" / "job.sub", sub_description)

    submit_cmd = condor.run_command(["condor_submit", submit_file])
    clusterid, num_procs = parse_submit_result(submit_cmd)

    jobids = [JobID(clusterid, n) for n in range(num_procs)]

    condor.job_queue.wait(
        {
            jobid: [
                (
                    SetJobStatus(JobStatus.RUNNING),
                    lambda j, e: condor.run_command(["condor_q"], echo=True),
                ),
                SetJobStatus(JobStatus.COMPLETED),
            ]
            for jobid in jobids
        },
        timeout=60,
    )

    yield jobids


@pytest.fixture(scope="class")
def num_jobs_running_history(
    condor, jobids_for_sleep_jobs, concurrency_limits_and_max_running
):
    _, max_running = concurrency_limits_and_max_running
    return track_quantity(
        condor.job_queue.filter(lambda j, e: j in jobids_for_sleep_jobs),
        increment_condition=lambda id_event: id_event[-1]
        == SetJobStatus(JobStatus.RUNNING),
        decrement_condition=lambda id_event: id_event[-1]
        == SetJobStatus(JobStatus.COMPLETED),
        max_quantity=max_running,
        expected_quantity=max_running,
    )


@pytest.fixture(scope="class")
def startd_log_file(condor):
    return condor.startd_log.open()


@pytest.fixture(scope="class")
def num_busy_slots_history(
    startd_log_file, jobids_for_sleep_jobs, concurrency_limits_and_max_running
):
    _, max_running = concurrency_limits_and_max_running

    logger.debug("Checking Startd log file...")
    logger.debug("Expected Job IDs are: {}".format(jobids_for_sleep_jobs))

    startd_log_file.read()

    active_claims_history = track_quantity(
        startd_log_file.lines,
        increment_condition=lambda line: "Changing activity: Idle -> Busy" in line,
        decrement_condition=lambda line: "Changing activity: Busy -> Idle" in line,
        max_quantity=max_running,
        expected_quantity=max_running,
    )

    startd_log_file.clear()

    return active_claims_history


class TestConcurrencyLimits:
    def test_all_jobs_ran(self, condor, jobids_for_sleep_jobs):
        for jobid in jobids_for_sleep_jobs:
            assert in_order(
                condor.job_queue.by_jobid[jobid],
                [
                    SetJobStatus(JobStatus.IDLE),
                    SetJobStatus(JobStatus.RUNNING),
                    SetJobStatus(JobStatus.COMPLETED),
                ],
            )

    def test_never_more_jobs_running_than_limit(
        self, num_jobs_running_history, concurrency_limits_and_max_running
    ):
        _, max_running = concurrency_limits_and_max_running
        assert max(num_jobs_running_history) <= max_running

    def test_num_jobs_running_hits_limit(
        self, num_jobs_running_history, concurrency_limits_and_max_running
    ):
        _, max_running = concurrency_limits_and_max_running
        assert max_running in num_jobs_running_history

    def test_never_more_busy_slots_than_limit(
        self, num_busy_slots_history, concurrency_limits_and_max_running
    ):
        _, max_running = concurrency_limits_and_max_running
        assert max(num_busy_slots_history) <= max_running

    def test_num_busy_slots_hits_limit(
        self, num_busy_slots_history, concurrency_limits_and_max_running
    ):
        _, max_running = concurrency_limits_and_max_running
        assert max_running in num_busy_slots_history
