#!/usr/bin/env pytest

# this test replicates the first part of job_late_materialize_py

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


@pytest.fixture(scope="class")
def condor(test_dir):
    with Condor(
        local_dir=test_dir / "condor",
        config={
            "NUM_CPUS": "5",
            "NUM_SLOTS": "5",  # must be larger than the max number of jobs we hope to materialize
            "SCHEDD_MATERIALIZE_LOG": "$(LOG)/MaterializeLog",
            "SCHEDD_DEBUG": "D_MATERIALIZE:2 D_CAT $(SCHEDD_DEBUG)",
        },
    ) as condor:
        yield condor


@pytest.fixture(scope="class")
def jobids(test_dir, condor):
    # enable late materialization, but high enough that they all
    # show up immediately
    sub_description = """
        executable = /bin/sleep
        arguments = 0

        request_memory = 1MB
        request_disk = 1MB

        max_materialize = 5

        My.Foo = "$(Item)"

        queue in (A, B, C, D, E)
    """
    submit_file = write_file(test_dir / "submit" / "job.sub", sub_description)

    submit_cmd = condor.run_command(["condor_submit", submit_file])
    clusterid, num_procs = get_submit_result(submit_cmd)

    jobids = [JobID(clusterid, n) for n in range(num_procs)]

    condor.job_queue.wait(
        {jobid: [SetJobStatus(JobStatus.Completed)] for jobid in jobids}, timeout=60
    )

    return jobids


@pytest.fixture(scope="class")
def num_materialized_jobs_history(condor, jobids_for_sleep_jobs):
    num_materialized = 0
    history = []
    for jobid, event in condor.job_queue.filter(
        lambda j, e: j in jobids_for_sleep_jobs
    ):
        if event == SetJobStatus(JobStatus.Idle):
            num_materialized += 1
        if event == SetJobStatus(JobStatus.Completed):
            num_materialized -= 1

        history.append(num_materialized)

    return history


@pytest.fixture(scope="class")
def num_idle_jobs_history(condor, jobids_for_sleep_jobs):
    num_idle = 0
    history = []
    for jobid, event in condor.job_queue.filter(
        lambda j, e: j in jobids_for_sleep_jobs
    ):
        if event == SetJobStatus(JobStatus.Idle):
            num_idle += 1
        if event == SetJobStatus(JobStatus.Running):
            num_idle -= 1

        history.append(num_idle)

    return history


class TestLateMaterializationLimits:
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

    def test_never_more_materialized_than_max_materialize(
        self, num_materialized_jobs_history, max_materialize
    ):
        assert max(num_materialized_jobs_history) <= max_materialize

    def test_hit_max_materialize_limit(
        self, num_materialized_jobs_history, max_materialize
    ):
        assert max(num_materialized_jobs_history) == max_materialize

    def test_never_more_idle_than_max_idle(
        self, num_idle_jobs_history, max_idle, max_materialize
    ):
        assert max(num_idle_jobs_history) <= min(max_idle, max_materialize)

    def test_hit_max_idle_limit(self, num_idle_jobs_history, max_idle, max_materialize):
        assert max(num_idle_jobs_history) == min(max_idle, max_materialize)
