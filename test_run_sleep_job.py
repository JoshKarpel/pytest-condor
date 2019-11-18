#!/usr/bin/env pytest

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import pytest

from harness import (
    write_file,
    get_submit_result,
    JobID,
    SetAttribute,
    SetJobStatus,
    JobStatus,
    in_order,
)


@pytest.fixture(scope="class")
def submit_sleep_job_cmd(default_condor, test_dir):
    sub_description = """
        executable = /bin/sleep
        arguments = 1
        
        queue
    """
    submit_file = write_file(test_dir / "submit" / "job.sub", sub_description)

    return default_condor.run_command(["condor_submit", submit_file])


@pytest.fixture(scope="class")
def finished_sleep_jobid(default_condor, submit_sleep_job_cmd):
    clusterid, num_procs = get_submit_result(submit_sleep_job_cmd)

    jobid = JobID(clusterid, 0)

    default_condor.wait_for_job_queue_events(
        expected_events={jobid: [SetJobStatus(JobStatus.Completed)]},
        unexpected_events={jobid: {SetJobStatus(JobStatus.Held)}},
    )

    return jobid


@pytest.fixture(scope="class")
def job_queue_events_for_sleep_job(default_condor, finished_sleep_jobid):
    return default_condor.get_job_queue_events()[finished_sleep_jobid]


class TestCanRunSleepJob:
    def test_submit_cmd_succeeded(self, submit_sleep_job_cmd):
        assert submit_sleep_job_cmd.returncode == 0

    def test_only_one_proc(self, submit_sleep_job_cmd):
        _, num_procs = get_submit_result(submit_sleep_job_cmd)
        assert num_procs == 1

    def test_job_queue_events_in_correct_order(self, job_queue_events_for_sleep_job):
        assert in_order(
            job_queue_events_for_sleep_job,
            [
                SetJobStatus(JobStatus.Idle),
                SetJobStatus(JobStatus.Running),
                SetJobStatus(JobStatus.Completed),
            ],
        )

    def test_job_executed_successfully(self, job_queue_events_for_sleep_job):
        assert SetAttribute("ExitCode", "0") in job_queue_events_for_sleep_job
