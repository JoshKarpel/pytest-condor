#!/usr/bin/env pytest

# this test replicates the first part of job_late_materialize_py

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import pytest

import htcondor

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
            "NUM_CPUS": "5",
            "NUM_SLOTS": "5",  # must be larger than the max number of jobs we hope to materialize
            "SCHEDD_MATERIALIZE_LOG": "$(LOG)/MaterializeLog",
            "SCHEDD_DEBUG": "D_MATERIALIZE:2 D_CAT $(SCHEDD_DEBUG)",
        },
    ) as condor:
        yield condor


_max_idle = [3, 2]


@pytest.fixture(
    scope="class", params=_max_idle, ids=["max_idle={}".format(p) for p in _max_idle]
)
def max_idle(request):
    return request.param


_max_materialize = [3, 2]


@pytest.fixture(
    scope="class",
    params=_max_materialize,
    ids=["max_materialize={}".format(p) for p in _max_idle],
)
def max_materialize(request):
    return request.param


@pytest.fixture(scope="class")
def jobids_for_sleep_jobs(test_dir, condor, max_idle, max_materialize):
    sub_description = """
        executable = /bin/sleep
        arguments = 3

        request_memory = 1MB
        request_disk = 1MB

        max_materialize = {max_materialize}
        max_idle = {max_idle}

        queue 5
    """.format(
        max_materialize=max_materialize, max_idle=max_idle
    )
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

    def test_never_more_materialized_than_max(
        self, num_materialized_jobs_history, max_materialize
    ):
        assert max(num_materialized_jobs_history) <= max_materialize

    def test_hit_max_materialize_limit(
        self, num_materialized_jobs_history, max_materialize
    ):
        assert max(num_materialized_jobs_history) == max_materialize

    def test_never_more_idle_than_max(
        self, num_idle_jobs_history, max_idle, max_materialize
    ):
        assert max(num_idle_jobs_history) <= min(max_idle, max_materialize)

    def test_hit_max_idle_limit(self, num_idle_jobs_history, max_idle, max_materialize):
        assert max(num_idle_jobs_history) == min(max_idle, max_materialize)


@pytest.fixture(scope="class")
def clusterid_for_itemdata(test_dir, condor):
    # enable late materialization, but with a high enough limit that they all
    # show up immediately (on hold, because we don't need to actually run
    # the jobs to do the tests)
    sub_description = """
        executable = /bin/sleep
        arguments = 0

        request_memory = 1MB
        request_disk = 1MB

        max_materialize = 5

        hold = true

        My.Foo = "$(Item)"

        queue in (A, B, C, D, E)
    """
    submit_file = write_file(test_dir / "submit" / "job.sub", sub_description)

    submit_cmd = condor.run_command(["condor_submit", submit_file])
    clusterid, num_procs = get_submit_result(submit_cmd)

    jobids = [JobID(clusterid, n) for n in range(num_procs)]

    condor.job_queue.wait(
        {jobid: [SetAttribute("Foo", None)] for jobid in jobids}, timeout=10
    )

    yield clusterid

    condor.run_command(["condor_rm", clusterid])


class TestLateMaterializationItemdata:
    def test_itemdata_turns_into_job_attributes(self, condor, clusterid_for_itemdata):
        actual = {}
        for jobid, event in condor.job_queue.filter(
            lambda j, e: j.cluster == clusterid_for_itemdata
        ):
            # the My. doesn't end up being part of the key in the jobad
            if event.matches(SetAttribute("Foo", None)):
                actual[jobid] = event.value

        expected = {
            # first item gets put on the clusterad!
            JobID(clusterid_for_itemdata, -1): '"A"',
            JobID(clusterid_for_itemdata, 1): '"B"',
            JobID(clusterid_for_itemdata, 2): '"C"',
            JobID(clusterid_for_itemdata, 3): '"D"',
            JobID(clusterid_for_itemdata, 4): '"E"',
        }

        assert actual == expected

    def test_query_produces_expected_results(self, condor, clusterid_for_itemdata):
        with condor.use_config():
            schedd = htcondor.Schedd()
            ads = schedd.query(
                constraint="clusterid == {}".format(clusterid_for_itemdata),
                # the My. doesn't end up being part of the key in the jobad
                attr_list=["clusterid", "procid", "foo"],
            )

        actual = [ad["foo"] for ad in sorted(ads, key=lambda ad: int(ad["procid"]))]
        expected = list("ABCDE")

        assert actual == expected
