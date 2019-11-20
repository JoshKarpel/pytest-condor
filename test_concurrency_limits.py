#!/usr/bin/env pytest

# this test replicates job_concurrency_limitsP

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import pytest

from harness import (
    Condor,
    write_file,
    get_submit_result,
    JobID,
    SetJobStatus,
    JobStatus,
    in_order,
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


@pytest.fixture(scope="class", params=SLOT_CONFIGS.values(), ids=SLOT_CONFIGS.keys())
def condor(request, test_dir):
    with Condor(
        local_dir=test_dir / "condor",
        config={
            **request.param,
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


@pytest.fixture(scope="class")
def startd_log_file(condor):
    with condor.startd_log.open(mode="r") as f:
        yield f


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
    concurrency_limits, max_running = concurrency_limits_and_max_running

    # we need the non-zero sleep to make sure we hit the concurrency limit
    sub_description = """
        executable = /bin/sleep
        arguments = 5
        
        request_memory = 1MB
        request_disk = 1MB

        concurrency_limits = {concurrency_limits}

        queue {num_jobs}
    """.format(
        concurrency_limits=concurrency_limits, num_jobs=max_running * 2
    )
    submit_file = write_file(test_dir / "submit" / "job.sub", sub_description)

    submit_cmd = condor.run_command(["condor_submit", submit_file])
    clusterid, num_procs = get_submit_result(submit_cmd)

    jobids = [JobID(clusterid, n) for n in range(num_procs)]

    condor.job_queue.wait(
        {jobid: [SetJobStatus(JobStatus.Completed)] for jobid in jobids}, timeout=60
    )

    yield jobids


@pytest.fixture(scope="class")
def num_jobs_running_history(condor, jobids_for_sleep_jobs):
    num_running = 0
    num_running_history = []
    for jobid, event in condor.job_queue.filter(
        lambda j, e: j in jobids_for_sleep_jobs
    ):
        if event == SetJobStatus(JobStatus.Running):
            num_running += 1
        elif event == SetJobStatus(JobStatus.Completed):
            num_running -= 1

        num_running_history.append(num_running)

    return num_running_history


@pytest.fixture(scope="class")
def num_busy_slots_history(
    startd_log_file, jobids_for_sleep_jobs, concurrency_limits_and_max_running
):
    _, max_running = concurrency_limits_and_max_running

    active_claims_history = []
    active_claims = 0

    for line in startd_log_file:
        line = line.strip()

        if "Changing activity: Idle -> Busy" in line:
            active_claims += 1
        elif "Changing activity: Busy -> Idle" in line:
            active_claims -= 1
        active_claims_history.append(active_claims)

        print(" {}/{} | {}".format(str(active_claims).rjust(3), max_running, line))

    return active_claims_history


class TestConcurrencyLimits:
    def test_all_jobs_ran(self, condor, jobids_for_sleep_jobs):
        for jobid in jobids_for_sleep_jobs:
            assert in_order(
                condor.job_queue.by_jobid[jobid],
                [
                    SetJobStatus(JobStatus.Idle),
                    SetJobStatus(JobStatus.Running),
                    SetJobStatus(JobStatus.Completed),
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
        assert max(num_jobs_running_history) == max_running

    def test_never_more_busy_slots_than_limit(
        self, num_busy_slots_history, concurrency_limits_and_max_running
    ):
        _, max_running = concurrency_limits_and_max_running
        assert max(num_busy_slots_history) <= max_running

    def test_num_busy_slots_hits_limit(
        self, num_busy_slots_history, concurrency_limits_and_max_running
    ):
        _, max_running = concurrency_limits_and_max_running
        assert max(num_busy_slots_history) == max_running
