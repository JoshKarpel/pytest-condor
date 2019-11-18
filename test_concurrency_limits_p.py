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
    SetAttribute,
    SetJobStatus,
    JobStatus,
    in_order,
)


@pytest.fixture(scope="class")
def condor(test_dir):
    with Condor(
        local_dir=test_dir / "condor",
        config={
            "NUM_CPUS": "12",  # should be larger than the number of jobs we plan to submit
            "SLOT_TYPE_1": "cpus=100%,memory=100%,disk=100%",
            "SLOT_TYPE_1_PARTITIONABLE": "True",
            "NUM_SLOTS_TYPE_1": "1",
            # below are the concurrency limits we'll test against
            "XSW_LIMIT": "2",
            "CONCURRENCY_LIMIT_DEFAULT": "2",
            "CONCURRENCY_LIMIT_DEFAULT_SMALL": "3",
            "CONCURRENCY_LIMIT_DEFAULT_LARGE": "1",
        },
    ) as condor:
        yield condor


@pytest.fixture(
    scope="class",
    params=[("XSW", 2), ("UNDEFINED:2", 1), ("small.license", 3), ("large.license", 1)],
)
def concurrency_limits_and_max_running(request):
    return request.param


@pytest.fixture(scope="class")
def jobids_for_sleep_jobs(test_dir, condor, concurrency_limits_and_max_running):
    concurrency_limits, _ = concurrency_limits_and_max_running

    # we need the long-ish sleep to make sure we hit the concurrency limit we
    # are aiming for
    sub_description = """
        executable = /bin/sleep
        arguments = 5
        request_memory = 1MB
        request_disk = 1MB

        concurrency_limits = {}

        queue 5
    """.format(
        concurrency_limits
    )
    submit_file = write_file(test_dir / "submit" / "job.sub", sub_description)

    submit_cmd = condor.run_command(["condor_submit", submit_file])
    clusterid, num_procs = get_submit_result(submit_cmd)

    jobids = [JobID(clusterid, n) for n in range(num_procs)]

    condor.job_queue.wait(
        {jobid: [SetJobStatus(JobStatus.Completed)] for jobid in jobids}, timeout=60
    )

    return jobids


class TestConcurrencyLimitsForPSlot:
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

    def test_never_more_jobs_running_than_concurrency_limit_allows(
        self, condor, jobids_for_sleep_jobs, concurrency_limits_and_max_running
    ):
        _, max_running = concurrency_limits_and_max_running

        num_running = 0
        num_running_history = []
        for jobid, event in condor.job_queue.filter(
            lambda j, e: j in jobids_for_sleep_jobs
        ):
            # we want to look at only jobs that were submitted in this run
            if event == SetJobStatus(JobStatus.Running):
                num_running += 1
            elif event == SetJobStatus(JobStatus.Completed):
                num_running -= 1

            num_running_history.append(num_running)

        # it should never be **more**
        assert max(num_running_history) <= max_running

        # and it should actually hit the max, given our config
        assert max(num_running_history) == max_running
